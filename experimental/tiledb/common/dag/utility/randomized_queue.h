/**
 * @file   randomized_queue.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares a classic/basic generic bounded-buffer producer-consumer
 * queue. The class supports a purely unbounded option.
 */

#ifndef TILEDB_RANDOMIZED_QUEUE_H
#define TILEDB_RANDOMIZED_QUEUE_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <random>

namespace tiledb::common {

/**
 * A simple queue that returns elements in random order.
 * Supports push, try_push, pop, try_pop, and shutdown.
 * @tparam Item type of item to store in the queue
 */
template <class Item>
class RandomizedQueue {
 public:
  RandomizedQueue() = default;

  RandomizedQueue(const RandomizedQueue&) = delete;
  RandomizedQueue& operator=(const RandomizedQueue&) = delete;

  /**
   * Copy Constructor
   */
  RandomizedQueue(RandomizedQueue&& rhs) noexcept
      : items_(std::move(rhs.items_))
      , draining_{rhs.draining_.load()}
      , shutdown_{rhs.shutdown_.load()} {
  }

  /**
   * Add an item to the queue.
   * @param item to push
   * @return always true
   */
  bool push(const Item& item) {
    std::unique_lock lock{mutex_};

    if (draining_ || shutdown_) {
      return false;
    }

    items_.push_back(item);

    empty_cv_.notify_one();
    return true;
  }

  /**
   * Here for historical reasons.  Queue is unbounded, so will always succeed.
   * @param item
   * @return true
   */
  bool try_push(const Item& item) {
    std::unique_lock lock{mutex_};

    if (draining_ || shutdown_) {
      return false;
    }

    items_.push_back(item);

    empty_cv_.notify_one();
    return true;
  }

  /**
   * Try to pop an item from the queue.  If the queue is empty, return nothing.
   * @return optional item
   */
  std::optional<Item> try_pop() {
    std::scoped_lock lock{mutex_};

    if (items_.empty() || draining_ || shutdown_) {
      return {};
    }

    shuffle_items();
    Item item = items_.back();
    items_.pop_back();

    return item;
  }

  /**
   * Pop an item from the queue.  If the queue is empty, wait until an item is
   * available.  If the queue is drained or shutdown, return empty optional.
   * @return optional item
   */
  std::optional<Item> pop() {
    std::unique_lock lock{mutex_};

    empty_cv_.wait(
        lock, [this]() { return !items_.empty() || draining_ || shutdown_; });

    if ((draining_ && items_.empty()) || shutdown_) {
      return {};
    }
    shuffle_items();
    Item item = items_.back();
    items_.pop_back();

    return item;
  }

  /**
   * Swap the data of this queue with the data of another queue.
   * @param rhs the other queue
   */
  void swap_data(RandomizedQueue& rhs) {
    std::scoped_lock lock{mutex_};
    std::swap(items_, rhs.queue_);
  }

  /**
   * Soft shutdown of the queue.  The queue is closed and all threads waiting on
   * items are notified.  Any threads waiting on pop() will then return nothing.
   */
  void drain() {
    std::scoped_lock lock{mutex_};
    draining_ = true;
    empty_cv_.notify_all();
  }

  /**
   * Hard shutdown of the queue.  The queue is closed and all threads waiting on
   * items are notified.  Any threads waiting on pop() will then return nothing.
   */
  void shutdown() {
    std::scoped_lock lock{mutex_};
    shutdown_ = true;
    empty_cv_.notify_all();
  }

  /**
   * Return size of (number of items in) the queue
   * @return
   */
  inline size_t size() const {
    return items_.size();
  }

  /**
   *
   * @return true if the queue is empty
   */

  inline bool empty() const {
    return items_.empty();
  }

 private:
  void shuffle_items() {
    static std::random_device rd;
    static std::mt19937 g(rd());
    std::shuffle(items_.begin(), items_.end(), g);
  }

  std::vector<Item> items_;
  std::condition_variable empty_cv_;

  mutable std::mutex mutex_;
  std::atomic<bool> draining_{false};
  std::atomic<bool> shutdown_{false};
};

}  // namespace tiledb::common

#endif  // TILEDB_RANDOMIZED_QUEUE_H
