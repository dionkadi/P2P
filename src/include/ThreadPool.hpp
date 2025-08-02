#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>
#include <condition_variable>

class ThreadPool {
  public:
    explicit ThreadPool(std::size_t);
    ~ThreadPool();

    template<typename F>
    void enqueue(F&& f) {
        auto task = std::make_unique<Task<F>>(std::forward<F>(f));
        {
            std::lock_guard lock(mutex_);
            tasks_.emplace(std::move(task));
        }
        condition_.notify_one();
    }

  private:
    struct ITask {
        virtual ~ITask() = default;
        virtual void execute() = 0;
    };

    template<class F>
    struct Task : ITask {
        F func;
        Task(F&& f) : func(std::move(f)) {}
        void execute() override { func(); }
    };

    std::vector<std::jthread> workers_;
    std::queue<std::unique_ptr<ITask>> tasks_;
    std::mutex mutex_;
    std::condition_variable condition_;
};
