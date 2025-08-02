#include "ThreadPool.hpp"
#include <memory>
#include <stop_token>


ThreadPool::ThreadPool(std::size_t num_threads) {
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

ThreadPool::~ThreadPool() {
    for (auto& worker : workers_) {
        worker.request_stop();
    }

    condition_.notify_all();
}