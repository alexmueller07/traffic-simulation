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