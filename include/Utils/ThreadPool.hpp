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
    explicit ThreadPool(std::size_t num_threads) {
        for (std::size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] (std::stop_token stop_token) {
                while (!stop_token.stop_requested()) {            
                    std::unique_ptr<ITask> task;
                    {
                        std::unique_lock lock(mutex_);
                        condition_.wait(lock, [this, &stop_token] {
                            return stop_token.stop_requested() || !tasks_.empty();
                        });

                        if (stop_token.stop_requested() && tasks_.empty()) {
                            return ;
                        }

                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    if (task)
                        task->execute();
                }
            });
        }
    }

    ~ThreadPool() {
        for (auto& worker : workers_) {
            worker.request_stop();
        }

        condition_.notify_all();
    }

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
