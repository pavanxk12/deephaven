// The one NATS connection every algo and the UI send portfolio commands through. A port of
// algo/comms/nats_service.py using the "clerk per portfolio" model agreed for the C++ side: Python's single asyncio
// loop + per-portfolio coroutine lanes become, here, one drain thread per active (user, pf) lane that runs that
// lane's jobs in order, while different portfolios overlap. A `Call` inside a job BLOCKS for its Reply, so a job
// reads top-to-bottom (subscribe -> pause -> apply -> unpause). max_in_flight caps the requests on the wire across
// all lanes (a counting semaphore). Control-plane / housekeeping: mutexes are fine here, never the hot path.
//
// The transport is a seam (like core/sql's SqlConn): a fake drives the tests; the real cnats transport is built when
// the NATS integration is done. JSON lives only in the real transport (the plan's "JSON at the edges"); the service
// passes a structured command (cmd + payload map) down, so the fake needs no JSON.
#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "core/pfstate.h"

namespace algo {

// A command payload value (what the engine's JSON carries): the fields are ints, doubles, strings or flags.
using PayloadValue = std::variant<int64_t, double, std::string, bool>;
using Payload = std::map<std::string, PayloadValue>;

// ---- the transport seam ------------------------------------------------------------------------------------------
struct Response { std::string status; std::map<std::string, std::string> body; };  // the bridge's Status + reply body

class NatsTimeout : public std::runtime_error { public: NatsTimeout() : std::runtime_error("timeout") {} };
class NatsNoResponders : public std::runtime_error { public: NatsNoResponders() : std::runtime_error("no_responders") {} };

class Transport {
public:
    virtual ~Transport() = default;
    virtual bool     isConnected() const { return true; }
    // Send `cmd`+`payload` to `subject`, wait up to `timeout_s`. Returns the bridge's Response, throws NatsTimeout /
    // NatsNoResponders / std::runtime_error on failure.
    virtual Response request(const std::string& subject, const std::string& cmd, const Payload& payload, double timeout_s) = 0;
    virtual void     close() {}
};

// ---- config and reply --------------------------------------------------------------------------------------------
struct NatsConfig {
    std::string          ip = "127.0.0.1";
    int32_t              port = 4222;
    std::vector<int64_t> users{3};              // the users that may send; a command names one (default when only one)
    std::string          sender = "AUTOMATION"; // stamped as SENDER (not on subscribe / unsubscribe)
    double               timeout_s = 5;         // per request, counted from the moment it goes on the wire
    int32_t              max_in_flight = 64;    // requests on the wire at once, all portfolios together; 0 = unlimited
    int32_t              lane_depth = 256;      // jobs waiting per portfolio
    int32_t              max_queued = 5000;     // commands inside the service (running + waiting); 0 = no cap
    std::string          subject = "cmd.hft.{pf}";
    bool                 dry_run = false;       // true: no connection, commands are only logged
    bool                 send_on = false;       // false: nothing is sent (switch live with sendOn())
    Payload              sub_payload;           // defaults merged into subscribe / unsubscribe (filled by default ctor)

    NatsConfig();
    std::string url() const { return "nats://" + ip + ":" + std::to_string(port); }
};

struct Reply {
    bool                               ok = false;
    std::string                        status;
    int64_t                            pf = 0;
    std::string                        cmd;
    std::map<std::string, std::string> data;
    int64_t                            user = 0;
    explicit operator bool() const { return ok; }
};

// status values for a job that never ran (a refusal), used by lane() to raise
bool isRefused(const std::string& status);

class NatsService;

// A blocking call inside a job: call(cmd, payload) -> Reply. One per (pf, user, client).
class Call {
public:
    Reply operator()(const std::string& cmd, const Payload& payload = {});
private:
    friend class NatsService;
    Call(NatsService* svc, int64_t pf, int64_t user, std::optional<std::string> client)
        : svc_(svc), pf_(pf), user_(user), client_(std::move(client)) {}
    NatsService*               svc_;
    int64_t                    pf_, user_;
    std::optional<std::string> client_;
};

using Job = std::function<Reply(Call&)>;        // a lane job: runs one or more Calls in order, returns the last Reply

class NatsService {
public:
    // `transport_factory` makes the transport (tests inject a fake). Without one and not dry_run, connect() throws.
    using TransportFactory = std::function<std::unique_ptr<Transport>()>;
    explicit NatsService(NatsConfig cfg = {}, TransportFactory factory = {});
    ~NatsService();

    // ---- lifecycle ----------------------------------------------------------------------------------------------
    void connect();                             // start the drain machinery and the transport (nothing in a dry run)
    void stop();                                // drain, tell waiting jobs "stopped", close the transport

    // ---- the six commands (each returns a future; .get() waits for the Reply) ------------------------------------
    std::future<Reply> subscribe(int64_t pf, int64_t option, const Payload& params = {}, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    std::future<Reply> unsubscribe(int64_t pf, int64_t option, const Payload& params = {}, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    std::future<Reply> pause(int64_t pf, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    std::future<Reply> unpause(int64_t pf, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    std::future<Reply> squareup(int64_t pf, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    std::future<Reply> apply(int64_t pf, const Payload& params, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);

    // ---- general entry points -----------------------------------------------------------------------------------
    std::future<Reply> send(int64_t pf, const std::string& cmd, const Payload& payload = {}, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    // Several commands back to back on one portfolio (gated by the send switches). Returns the future of the job.
    std::future<Reply> submit(int64_t pf, Job job, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt, const std::string& label = "job");
    // For use inside a workflow started by go(): run `job` on pf's lane and BLOCK for its result (not gated by the
    // switches). Throws std::runtime_error if the job was refused.
    Reply              lane(int64_t pf, Job job, std::optional<int64_t> user = std::nullopt, std::optional<std::string> client = std::nullopt);
    // Run a free-form workflow (that calls lane() itself) on its own thread. Returns false (dropped) while sending is
    // off for everyone or for this client. The workflow's threads are joined at stop.
    bool               go(std::function<void()> workflow, std::optional<std::string> client = std::nullopt);

    // ---- the switches -------------------------------------------------------------------------------------------
    bool sendOn();                              // master switch on (needs a live transport)
    void sendOff();
    bool enable(const std::string& client);     // a client's own switch on
    void disable(const std::string& client);
    bool isEnabled(const std::string& client) const;
    bool allowed(std::optional<std::string> client = std::nullopt) const;   // master on AND the client's switch on
    void addUsers(const std::vector<int64_t>& users);

    // ---- users and blocks ---------------------------------------------------------------------------------------
    void block(std::optional<int64_t> user, const std::vector<int64_t>& pfs = {});
    void unblock(std::optional<int64_t> user, const std::vector<int64_t>& pfs = {});

    // ---- state --------------------------------------------------------------------------------------------------
    bool                   connected() const;
    std::string            state() const;       // service_down / dry / disconnected / off / on
    std::optional<PfState> pfState(int64_t pf, std::optional<int64_t> user = std::nullopt) const;
    PfStates&              states() { return states_; }
    const NatsConfig&      cfg() const { return cfg_; }
    bool                   sending() const { return sending_.load(); }

    // counters (read for tests / stats)
    struct Counts { int64_t sent = 0, ok = 0, failed = 0, timeout = 0, dropped = 0; };
    Counts  counts() const;
    int64_t inFlight() const { return in_flight_.load(); }
    int64_t queued() const { return queued_.load(); }

    // one nats_events row: (user, pf, cmd, status, detail). Kept in memory here (the DH table comes with the API).
    struct Event { int64_t user, pf; std::string cmd, status, detail; };
    std::vector<Event> events() const;
    void               note(int64_t pf, const std::string& cmd, const std::string& status, const std::string& detail = "", int64_t user = 0);

private:
    friend class Call;

    struct Waiting { Job job; std::promise<Reply> promise; int64_t user; std::optional<std::string> client; };

    std::future<Reply> submitInternal(int64_t pf, Job job, const std::string& label, std::optional<int64_t> user, bool gated, std::optional<std::string> client);
    void               enqueue(int64_t pf, int64_t user, Job job, std::promise<Reply> promise, std::optional<std::string> client);
    void               drain(std::pair<int64_t, int64_t> key);         // the drain thread body for one lane
    Reply              call(int64_t pf, int64_t user, const std::optional<std::string>& client, const std::string& cmd, const Payload& payload);
    std::pair<int64_t, std::string> resolve(std::optional<int64_t> user) const;   // (user, "") or (0, why)
    Payload            stamp(const std::string& cmd, Payload payload, int64_t user) const;
    void               track(int64_t pf, int64_t user, const std::string& cmd, const Payload& payload, const std::optional<std::string>& client);
    void               acquireSlot();
    void               releaseSlot();

    NatsConfig                        cfg_;
    TransportFactory                  factory_;
    std::unique_ptr<Transport>        transport_;
    std::atomic<bool>                 sending_{false};
    std::atomic<bool>                 closing_{false};
    std::atomic<bool>                 stopped_{true};

    mutable std::mutex                mu_;        // guards lanes_, enabled_, blocks, counts_, events_, drain count
    std::map<std::pair<int64_t, int64_t>, std::deque<Waiting>> lanes_;   // (user, pf) -> waiting jobs
    int                               active_drains_ = 0;                // live drain threads (detached)
    std::condition_variable           drains_cv_;                        // guarded by mu_; stop() waits on it
    std::vector<std::thread>          workflows_;                        // go() threads, joined at stop
    std::set<std::string>             enabled_;
    std::set<int64_t>                 blocked_users_;
    std::set<std::pair<int64_t, int64_t>> blocked_pfs_;
    Counts                            counts_;
    std::vector<Event>                events_;
    PfStates                          states_;

    // the in-flight cap: a counting semaphore built from max_in_flight (0 = unlimited)
    std::mutex                        slot_mu_;
    std::condition_variable           slot_cv_;
    int32_t                           slots_ = 0;
    std::atomic<int64_t>              in_flight_{0};
    std::atomic<int64_t>              queued_{0};
    std::atomic<int64_t>              running_{0};
};

}  // namespace algo
