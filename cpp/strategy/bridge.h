// The strategy's command sequences, sent through the shared NatsService. A port of algo/strategy/bridge.py. The
// Bridge holds no connection of its own: it knows the portfolio book and the ORDER of commands for a swap, a retire,
// an exit; the service does the sending, keeps one portfolio's commands in order and lets portfolios overlap.
//
// Each bridge is one named client of the service with its own send switch (starts OFF), so one algo stopping never
// stops another. A command goes out only while the service's master switch AND this bridge's switch are on. The
// workflows (swap/retire/exit/unsubscribe) run on a NatsService go() thread and drive lanes with blocking Calls, so
// each reads top-to-bottom; a failure leaves the portfolio paused and rolls the bookkeeping back. Control-plane.
#pragma once
#include <cstdint>
#include <functional>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "comms/nats_service.h"
#include "strategy/book.h"

namespace algo {

// How a strategy sequences its commands (the connection itself is NatsConfig). Port of algo/strategy/config.py.
struct BridgeConfig {
    bool                   unsubscribe_on_retire = true;   // after the squareup, unsubscribe the old portfolio
    bool                   squareup_on_repoint = true;     // square up the old portfolio when a stock moves option
    double                 squareup_settle_s = 60.0;       // wait after a squareup before the unsubscribe destroys it
    int64_t                add_qty = 5;                     // added to the cumulative maxqty on every activation/flip
    Payload                subscribe;                       // fields added over the service's subscribe defaults
    std::optional<int64_t> user;                            // the user this automation sends as
};

class Bridge {
public:
    using Token = uint64_t;

    explicit Bridge(BridgeConfig cfg = {}, std::string client = "bridge");

    // Wire the service and the book. Registers the pf-state listener and adopts what the book already had subscribed.
    void bind(NatsService* nats, Portfolios* book);
    void unbind();                                           // drop the listener (a stopped strategy)

    // ---- the send switch ----------------------------------------------------------------------------------------
    bool sendOn();                                           // this algo may send (starts OFF)
    void sendOff();                                          // keeps computing, sends nothing; others unaffected
    bool sending() const;                                    // master switch AND this bridge's switch both on
    bool dryRun() const;
    std::string state() const;                               // service_down / disconnected / dry / off / on
    Token onEnable(std::function<void()> cb);                // after sendOn(): re-check tokens sitting in a signal zone
    Token onHalt(std::function<void()> cb);                  // from unsubscribeAll(): stop producing signals
    void  removeEnable(Token t);
    void  removeHalt(Token t);

    // ---- command sequences (start a workflow; return true if it was started) ------------------------------------
    bool retire(int64_t old_pf);
    bool swap(int64_t pf, int64_t token, int64_t option, Payload params,
              std::optional<std::string> prev_leg, std::optional<int64_t> prev_option,
              std::optional<int64_t> old_pf = std::nullopt);

    // ---- operator actions ---------------------------------------------------------------------------------------
    std::optional<std::future<Reply>> pausePf(int64_t pf);
    std::optional<std::future<Reply>> unpausePf(int64_t pf);
    std::optional<std::future<Reply>> squareupPf(int64_t pf);   // the console only allows this while the PF is paused
    void pauseAll();
    void unpauseAll();                                       // current portfolios only; a retired one stays paused
    void exitAll(bool pause = true);                         // square up every portfolio
    std::vector<std::future<Reply>> unsubscribeAll(bool pause = true);  // stop signals, square up + unsubscribe each

    // ---- lookups ------------------------------------------------------------------------------------------------
    std::optional<int64_t> user() const;                    // cfg.user, else the book's user
    std::optional<int64_t> subscribedOption(int64_t pf) const;

private:
    void    onPf(int64_t user, int64_t pf, const PfState& st);   // a confirmed command: bring the book in line
    Payload sub(int64_t option) const;                          // subscribe/unsubscribe payload (defaults + T1)
    Reply   pauseStep(Call& call, int64_t pf, const std::string& action);  // call + book update when ok
    bool    guard() const;                                      // may this bridge send right now?

    BridgeConfig            cfg_;
    std::string             client_;
    NatsService*            nats_ = nullptr;
    Portfolios*             book_ = nullptr;
    PfStates::Token         listener_ = 0;

    mutable std::mutex                              cb_mu_;
    std::vector<std::pair<Token, std::function<void()>>> enable_cbs_, halt_cbs_;
    Token                                           next_token_ = 1;
};

}  // namespace algo
