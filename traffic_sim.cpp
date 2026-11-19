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