#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

static const Clock::time_point START = Clock::now();
static std::atomic<bool> running{true};
static std::atomic<int> p3Total{0};

static std::mutex logMutex;

static void log(const std::string& who, const std::string& msg) {
    long ms = std::chrono::duration_cast<milliseconds>(Clock::now() - START).count();
    std::ostringstream line;
    line << "[" << std::setw(6) << ms << " ms] " << std::left << std::setw(8) << who
         << " " << msg << "\n";
    std::lock_guard<std::mutex> lk(logMutex);
    std::cout << line.str() << std::flush;
}

class Storage {
public:
    Storage(std::string name, int capacity) : name_(std::move(name)), capacity_(capacity) {}
    int putOne(const std::string& who) {
        std::unique_lock<std::mutex> lk(m_);
        if (count_ == capacity_) log(who, name_ + " storage full -> production paused");
        notFull_.wait(lk, [&] { return count_ < capacity_ || !running; });
        if (!running) return -1;
        ++count_;
        notEmpty_.notify_all();
        return count_;
    }
    int takeExactly(int n) {
        std::unique_lock<std::mutex> lk(m_);
        notEmpty_.wait(lk, [&] { return count_ >= n || !running; });
        if (!running) return 0;
        count_ -= n;
        notFull_.notify_all();
        return n;
    }
    int takeUpTo(int max) {
        std::unique_lock<std::mutex> lk(m_);
        notEmpty_.wait(lk, [&] { return count_ >= 1 || !running; });
        if (!running) return 0;
        int n = count_ < max ? count_ : max;
        count_ -= n;
        notFull_.notify_all();
        return n;
    }

    int count() {
        std::lock_guard<std::mutex> lk(m_);
        return count_;
    }

    void wakeAll() {
        std::lock_guard<std::mutex> lk(m_);
        notFull_.notify_all();
        notEmpty_.notify_all();
    }

private:
    std::string name_;
    const int capacity_;
    int count_ = 0;
    std::mutex m_;
    std::condition_variable notFull_, notEmpty_;
};

class F3Input {
public:
    static const int CAP_P1 = 10, CAP_P2 = 5;

    bool putP1(const std::string& who) { return put(who, p1_, CAP_P1, "P1"); }
    bool putP2(const std::string& who) { return put(who, p2_, CAP_P2, "P2"); }

    bool takeInputs(const std::string& who) {
        std::unique_lock<std::mutex> lk(m_);
        if (!(p1_ >= 2 && p2_ >= 1))
            log(who, "waiting for inputs (P1=" + std::to_string(p1_) +
                         " P2=" + std::to_string(p2_) + ")");
        inputs_.wait(lk, [&] { return (p1_ >= 2 && p2_ >= 1) || !running; });
        if (!running) return false;
        p1_ -= 2;
        p2_ -= 1;
        space_.notify_all();
        return true;
    }

    std::string levels() {
        std::lock_guard<std::mutex> lk(m_);
        return "P1=" + std::to_string(p1_) + "/10 P2=" + std::to_string(p2_) + "/5";
    }

    void wakeAll() {
        std::lock_guard<std::mutex> lk(m_);
        inputs_.notify_all();
        space_.notify_all();
    }

private:
    bool put(const std::string& who, int& slot, int cap, const char* name) {
        std::unique_lock<std::mutex> lk(m_);
        if (slot == cap) log(who, std::string("F3 ") + name + " store full -> waiting to unload");
        space_.wait(lk, [&] { return slot < cap || !running; });
        if (!running) return false;
        ++slot;
        inputs_.notify_all();
        return true;
    }

    int p1_ = 0, p2_ = 0;
    std::mutex m_;
    std::condition_variable inputs_, space_;
};

class Gate {
public:
    void setDown(bool down) {
        std::lock_guard<std::mutex> lk(m_);
        down_ = down;
        cv_.notify_all();
    }

    void passIfUp(const std::string& who) {
        std::unique_lock<std::mutex> lk(m_);
        if (!down_) return;
        log(who, "OUT OF SERVICE");
        cv_.wait(lk, [&] { return !down_ || !running; });
        if (running) log(who, "back in service");
    }

    void wakeAll() {
        std::lock_guard<std::mutex> lk(m_);
        cv_.notify_all();
    }

private:
    bool down_ = false;
    std::mutex m_;
    std::condition_variable cv_;
};

static Storage f1Store("F1", 10);
static Storage f2Store("F2", 5);
static F3Input f3In;
static Gate f1Gate, f2Gate, shipGate, dept1Gate, dept2Gate;

static void producer(const std::string name, const std::string product, Storage& out,
                     int periodMs, int capacity, Gate& gate) {
    int made = 0;
    while (running) {
        gate.passIfUp(name);
        if (!running) break;
        std::this_thread::sleep_for(milliseconds(periodMs));
        int level = out.putOne(name);
        if (level < 0) break;
        log(name, "produced " + product + " #" + std::to_string(++made) + " (storage " +
                      std::to_string(level) + "/" + std::to_string(capacity) + ")");
    }
}

static void ship() {
    const std::string me = "Ship";
    while (running) {
        shipGate.passIfUp(me);
        int n = f1Store.takeUpTo(5); 
        if (n == 0) break;
        log(me, "loaded " + std::to_string(n) + "xP1 at F1, sailing to F3");
        std::this_thread::sleep_for(milliseconds(250));
        for (int i = 0; i < n; ++i)
            if (!f3In.putP1(me)) return;
        log(me, "unloaded " + std::to_string(n) + "xP1 at F3 (" + f3In.levels() + "), returning");
        std::this_thread::sleep_for(milliseconds(250));
    }
}

static void truck(int id) {
    const std::string me = "Truck" + std::to_string(id);
    while (running) {
        if (f2Store.takeExactly(2) == 0) break; 
        log(me, "loaded 2xP2 at F2, driving to F3");
        std::this_thread::sleep_for(milliseconds(250));
        for (int i = 0; i < 2; ++i)
            if (!f3In.putP2(me)) return;
        log(me, "unloaded 2xP2 at F3 (" + f3In.levels() + "), returning");
        std::this_thread::sleep_for(milliseconds(250));
    }
}

static void department(int id, Gate& gate) {
    const std::string me = "F3-D" + std::to_string(id);
    while (running) {
        gate.passIfUp(me);
        if (!running) break;
        if (!f3In.takeInputs(me)) break;
        std::this_thread::sleep_for(milliseconds(400));
        log(me, "produced P3 (total " + std::to_string(++p3Total) + ")");
    }
}

struct FaultEvent {
    int second;
    Gate* gate;
    bool down;
    const char* text;
};

int main() {
    const int RUN_SECONDS = 32;
    const FaultEvent schedule[] = {
        {4, &f1Gate, true, "FAULT: F1 production stopped"},
        {7, &f1Gate, false, "REPAIR: F1 production resumed"},
        {10, &f2Gate, true, "FAULT: F2 production stopped"},
        {15, &f2Gate, false, "REPAIR: F2 production resumed"},
        {18, &shipGate, true, "FAULT: ship out of commission"},
        {21, &shipGate, false, "REPAIR: ship back in commission"},
        {24, &dept1Gate, true, "FAULT: F3 department 1 out of order"},
        {27, &dept1Gate, false, "REPAIR: F3 department 1 repaired"},
    };

    std::vector<std::thread> threads;
    threads.emplace_back(producer, "F1", "P1", std::ref(f1Store), 100, 10, std::ref(f1Gate));
    threads.emplace_back(producer, "F2", "P2", std::ref(f2Store), 200, 5, std::ref(f2Gate));
    threads.emplace_back(ship);
    for (int i = 1; i <= 5; ++i) threads.emplace_back(truck, i);
    threads.emplace_back(department, 1, std::ref(dept1Gate));
    threads.emplace_back(department, 2, std::ref(dept2Gate));

    int lastTotal = 0;
    for (int sec = 1; sec <= RUN_SECONDS; ++sec) {
        std::this_thread::sleep_until(START + std::chrono::seconds(sec));
        for (const FaultEvent& e : schedule) {
            if (e.second == sec) {
                log("CONTROL", e.text);
                e.gate->setDown(e.down);
            }
        }
        int total = p3Total;
        log("STATS", "t=" + std::to_string(sec) + "s  P3 this second=" +
                         std::to_string(total - lastTotal) + "/5  total=" + std::to_string(total) +
                         "  F1=" + std::to_string(f1Store.count()) + "/10 F2=" +
                         std::to_string(f2Store.count()) + "/5 F3 " + f3In.levels());
        lastTotal = total;
    }

    running = false;
    f1Store.wakeAll();
    f2Store.wakeAll();
    f3In.wakeAll();
    for (Gate* g : {&f1Gate, &f2Gate, &shipGate, &dept1Gate, &dept2Gate}) g->wakeAll();
    for (std::thread& t : threads) t.join();

    int total = p3Total;
    double maxPossible = RUN_SECONDS * 5.0;
    std::ostringstream s;
    s << std::fixed << std::setprecision(1) << "finished: " << total << " P3 in " << RUN_SECONDS
      << " s (theoretical max " << maxPossible << ", efficiency " << 100.0 * total / maxPossible
      << "% incl. startup and all 4 faults)";
    log("STATS", s.str());
    return 0;
}
