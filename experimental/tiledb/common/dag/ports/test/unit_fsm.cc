/**
 * @file unit_fsm.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * Tests the ports finite state machine.
 */

#include "unit_fsm.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/ports/fsm.h"

using namespace tiledb::common;

/**
 * A series of helperf functions for testing the state
 * of the finite-state machine. We work with strings instead
 * of the enum values in order to make the printed output
 * from failed tests more interpretable.
 *
 * The functions are used as
 *
 * CHECK(is_src_empty(state) == "");
 *
 * If the condition passes, an empty string is returned,
 * otherwise, the string that is passed in is returned and
 * the CHECK will print its value in the diagnostic message.
 */
std::string is_src_empty(PortState st) {
  if (str(st) == "empty_full" || str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_src_full(PortState st) {
  if (str(st) == "full_full" || str(st) == "full_empty") {
    return {};
  }
  return str(st);
}

std::string is_src_post_swap(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_full" ||
      str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_snk_empty(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_snk_full(PortState st) {
  if (str(st) == "full_full" || str(st) == "empty_full") {
    return {};
  }
  return str(st);
}

std::string is_snk_post_swap(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_full" ||
      str(st) == "full_full") {
    return {};
  }
  return str(st);
}

/**
 * A function to generate a random number between 0 and the specified max
 */
size_t random_us(size_t max = 7500) {
  thread_local static uint64_t generator_seed =
      std::hash<std::thread::id>()(std::this_thread::get_id());
  thread_local static std::mt19937_64 generator(generator_seed);
  std::uniform_int_distribution<size_t> distribution(0, max);
  return distribution(generator);
}

// using PortStateMachine = NullStateMachine;
using PortStateMachine = DebugStateMachine<size_t>;

TEST_CASE("Port FSM: Construct", "[fsm]") {
  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(a.state() == PortState::empty_empty);
}

TEST_CASE("Port FSM: Start up", "[fsm]") {
  constexpr bool debug = false;
  [[maybe_unused]] auto a = PortStateMachine{};

  a.debug_ = debug;
  CHECK(a.state() == PortState::empty_empty);

  SECTION("start source") {
    a.event(PortEvent::source_fill, debug ? "start source" : "");
    CHECK(a.state() == PortState::full_empty);
  }

  SECTION("start sink") {
    a.event(PortEvent::source_fill, debug ? "start sink (fill)" : "");
    CHECK(str(a.state()) == "full_empty");
    a.event(PortEvent::push, debug ? "start sink (push)" : "");
    CHECK(is_src_empty(a.state()) == "");
    a.event(PortEvent::sink_drain, debug ? "start sink (drain)" : "");
    CHECK(is_snk_empty(a.state()) == "");
  }
}

TEST_CASE("Port FSM: Basic manual sequence", "[fsm]") {
  [[maybe_unused]] auto a = PortStateMachine{};
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::push);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::push);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "empty_empty");

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::pull);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::pull);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::push);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::pull);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::pull);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::push);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);
}

/**
 * An asynchronous state machine class.  Implements on_sink_swap and
 * on_source_swap using locks and condition variables.
 */
template <class T>
class AsyncStateMachine : public PortFiniteStateMachine<AsyncStateMachine<T>> {
  using FSM = PortFiniteStateMachine<AsyncStateMachine<T>>;
  //  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  int source_swaps{};
  int sink_swaps{};

  T source_item;
  T sink_item;

  bool debug_{false};

  using lock_type = typename FSM::lock_type;

  AsyncStateMachine(T source_init, T sink_init, bool debug = false)
      : source_item(source_init)
      , sink_item(sink_init)
      , debug_(debug) {
    //    CHECK(str(FSM::state()) == "empty_empty");

    FSM::debug_ = debug;
    if (debug_)
      std::cout << "\nConstructing AsyncStateMachine" << std::endl;
  }

  AsyncStateMachine(const AsyncStateMachine&) = default;
  AsyncStateMachine(AsyncStateMachine&&) = default;

  inline void on_ac_return(lock_type&, int) {
  }

  inline void notify_source(lock_type&, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " sink notifying source (on_signal_source) with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    source_cv_.notify_one();
  }

  inline void notify_sink(lock_type&, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " source notifying sink(on_signal_sink) with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    sink_cv_.notify_one();
  }

  inline void on_sink_swap(lock_type& lock, std::atomic<int>& event) {
    CHECK(is_snk_empty(FSM::state()) == "");

    if (FSM::state() == PortState::full_empty) {
      CHECK(str(FSM::state()) == "full_empty");

      std::swap(source_item, sink_item);

      if (debug_)
        std::cout << event++ << "  "
                  << "  sink notifying source (swap) with " +
                         str(FSM::state()) + " and " + str(FSM::next_state())
                  << std::endl;
      source_cv_.notify_one();
      FSM::set_state(PortState::empty_full);
      FSM::set_next_state(PortState::empty_full);

      if (debug_)
        std::cout << event++ << "  "
                  << " sink done swapping items with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;

      sink_swaps++;
    } else {
      if (debug_)
        std::cout << event++ << "  "
                  << " sink notifying source(drained) with " +
                         str(FSM::state()) + " and " + str(FSM::next_state())
                  << std::endl;
      source_cv_.notify_one();

      if (debug_)
        std::cout << event++ << "  "
                  << " sink going to sleep on_sink_swap with " +
                         str(FSM::state())
                  << std::endl;
      sink_cv_.wait(lock);

      FSM::set_next_state(FSM::state());

      if (debug_)
        std::cout << event++ << "  "
                  << " sink waking up on_sink_swap with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;

      if (debug_)
        std::cout << event++ << "  "
                  << " sink leaving on_sink_swap with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;
    }
  }

  inline void on_source_swap(lock_type& lock, std::atomic<int>& event) {
    CHECK(is_src_full(FSM::state()) == "");
    if (FSM::state() == PortState::full_empty) {
      CHECK(str(FSM::state()) == "full_empty");

      if (debug_)
        std::cout << event++ << "  "
                  << " source swapping items with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;
      std::swap(source_item, sink_item);

      if (debug_)
        std::cout << event++ << "  "
                  << " source notifying sink (swap) with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;
      sink_cv_.notify_one();

      FSM::set_state(PortState::empty_full);
      FSM::set_next_state(PortState::empty_full);

      if (debug_)
        std::cout << event++ << "  "
                  << " source done swapping items with " + str(FSM::state()) +
                         " and " + str(FSM::next_state())
                  << std::endl;

      source_swaps++;
    } else {
      CHECK(str(FSM::state()) == "full_full");
      if (debug_)
        std::cout << event++ << "  "
                  << " source notifying sink (filled) with " +
                         str(FSM::state()) + " and " + str(FSM::next_state())
                  << std::endl;
      sink_cv_.notify_one();

      if (debug_)
        std::cout << event++ << "  "
                  << " source going to sleep on_source_swap with " +
                         str(FSM::state()) + " and " + str(FSM::next_state())
                  << std::endl;

      source_cv_.wait(lock);

      FSM::set_next_state(FSM::state());

      if (debug_)
        std::cout << event++ << "  "
                  << " source waking up to " + str(FSM::state()) + " and " +
                         str(FSM::next_state())
                  << std::endl;

      if (debug_)
        std::cout << event++ << "  "
                  << " source leaving on_source_swap with " +
                         str(FSM::state()) + " and " + str(FSM::next_state())
                  << std::endl;
    }
  }
};

/**
 * An asynchronous state machine class, using only one cv and using the same
 * function for source_swap and sink_swap.
 */
template <class T>
class UnifiedAsyncStateMachine
    : public PortFiniteStateMachine<UnifiedAsyncStateMachine<T>> {
  using FSM = PortFiniteStateMachine<UnifiedAsyncStateMachine<T>>;
  using lock_type = typename FSM::lock_type;
  std::condition_variable cv_;

 public:
  int source_swaps{};
  int sink_swaps{};

  T source_item;
  T sink_item;

  bool debug_{false};

  UnifiedAsyncStateMachine(T source_init, T sink_init, bool debug = false)
      : source_item(source_init)
      , sink_item(sink_init)
      , debug_{debug} {
    //    CHECK(str(FSM::state()) == "empty_empty");
    FSM::debug_ = debug;
    if (debug_)
      std::cout << "\nConstructing UnifiedAsyncStateMachine" << std::endl;
  }

  UnifiedAsyncStateMachine(const UnifiedAsyncStateMachine&) = default;
  UnifiedAsyncStateMachine(UnifiedAsyncStateMachine&&) = default;

  inline void on_ac_return(lock_type&, int){};

  inline void do_notify(lock_type&, std::atomic<int>&) {
    cv_.notify_one();
  }

  inline void notify_source(lock_type& lock, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " sink notifying source" << std::endl;
    do_notify(lock, event);
  }
  inline void notify_sink(lock_type& lock, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " source notifying sink" << std::endl;
    do_notify(lock, event);
  }

  inline void on_source_swap(lock_type& lock, std::atomic<int>& event) {
    if (FSM::state() == PortState::full_empty) {
      if (debug_)
        std::cout << event++ << "  "
                  << " source swapping items " << source_item << " and "
                  << sink_item << std::endl;
      std::swap(source_item, sink_item);

      if (debug_)
        std::cout << event++ << "  "
                  << " source notifying sink (swap)" << std::endl;

      cv_.notify_one();

      FSM::set_state(PortState::empty_full);
      FSM::set_next_state(PortState::empty_full);
      source_swaps++;
    } else {
      if (debug_)
        std::cout << event++ << "  "
                  << " source notifying sink (filled)" << std::endl;
      cv_.notify_one();
      cv_.wait(lock);

      FSM::set_next_state(FSM::state());
    }
  }
  inline void on_sink_swap(lock_type& lock, std::atomic<int>& event) {
    on_source_swap(lock, event);
  }
};

TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and manual sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0};

  a.set_state(PortState::empty_empty);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
    CHECK(is_src_full(a.state()) == "");
    a.event(PortEvent::push, debug ? "async source (push)" : "");
    CHECK(is_src_empty(a.state()) == "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call drained" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.event(PortEvent::pull, debug ? "manual sink (pull)" : "");
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain, debug ? "manual sink (drain)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "AsynchronousStateMachine: Manual source and asynchronous sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
    CHECK(is_snk_full(a.state()) == "");

    a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.event(PortEvent::source_fill, debug ? "manual source (fill)" : "");
  a.event(PortEvent::push, debug ? "manual source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and manual sink",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = UnifiedAsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.event(PortEvent::source_fill, debug ? "manual async source (fill)" : "");
    a.event(PortEvent::push, debug ? "manual async source (push)" : "");
  });

  if (debug)
    std::cout << "About to call drained" << std::endl;

  a.event(PortEvent::pull, debug ? "manual async sink (pull)" : "");
  a.event(PortEvent::sink_drain, debug ? "manual async sink (drained)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "UnifiedAsynchronousStateMachine: Manual source and asynchronous sink",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = UnifiedAsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.event(PortEvent::pull, debug ? "manual async sink (pull)" : "");
    a.event(PortEvent::sink_drain, debug ? "manual async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.event(PortEvent::source_fill, debug ? "manual async source (fill)" : "");
  a.event(PortEvent::push, debug ? "manual async source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and asynchronous sink",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async dsink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }
  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = UnifiedAsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.event(PortEvent::pull, debug ? "async sink (pull)" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.event(PortEvent::source_fill, debug ? "async source (fill)" : "");
      a.event(PortEvent::push, debug ? "async source (push)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and asynchronous sink, n "
    "iterations",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0, debug};

  a.debug_ = debug;

  a.set_state(PortState::empty_empty);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::source_fill, debug ? "async source node" : "");
      a.event(PortEvent::push, debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::pull, debug ? "async sink node" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink node" : "");
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink, n iterations",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = UnifiedAsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::source_fill, debug ? "async source node" : "");
      a.event(PortEvent::push, debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::pull, debug ? "async sink node" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink node" : "");
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }
  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink, n iterations, no sleeping",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::source_fill, debug ? "async source node" : "");
      a.event(PortEvent::push, debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.event(PortEvent::pull, debug ? "async sink node" : "");
      a.event(PortEvent::sink_drain, debug ? "async sink node" : "");
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }
  CHECK(str(a.state()) == "empty_empty");
};

TEST_CASE("Pass a sequence of n integers, async", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = AsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      while (a.state() == PortState::full_empty ||
             a.state() == PortState::full_full)
        ;

      CHECK(is_src_empty(a.state()) == "");

      a.source_item = *i++;

      a.event(PortEvent::source_fill, debug ? "async source node" : "");
      a.event(PortEvent::push, debug ? "async source node" : "");

      a.source_item = 400000000;
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      while (a.state() == PortState::full_full ||
             a.state() == PortState::empty_full)
        ;

      a.event(PortEvent::pull, debug ? "async sink node" : "");

      CHECK(is_snk_full(a.state()) == "");

      a.event(PortEvent::sink_drain, debug ? "async sink node" : "");
      *j++ = a.sink_item;
      a.sink_item = 1000000000;
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  if (debug)
    for (size_t i = 0; i < rounds; ++i) {
      std::cout << i << " (" << input[i] << ", " << output[i] << ")"
                << std::endl;
    }

  if (!std::equal(input.begin(), input.end(), output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }
  if (!std::equal(input.begin(), input.end(), output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  REQUIRE(std::equal(input.begin(), input.end(), output.begin()));
}

TEST_CASE("Pass a sequence of n integers, unified", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = UnifiedAsyncStateMachine{0, 0, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      while (a.state() == PortState::full_empty ||
             a.state() == PortState::full_full)
        ;

      CHECK(is_src_empty(a.state()) == "");

      a.source_item = *i++;
      a.event(PortEvent::source_fill, debug ? "async source node" : "");
      a.event(PortEvent::push, debug ? "async source node" : "");

      CHECK(is_src_empty(a.state()) == "");

      a.source_item = 400000000;
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }

      while (a.state() == PortState::full_full ||
             a.state() == PortState::empty_full)
        ;

      a.event(PortEvent::pull, debug ? "async sink node" : "");

      CHECK(is_snk_full(a.state()) == "");

      a.event(PortEvent::sink_drain, debug ? "async sink node" : "");
      *j++ = a.sink_item;
      a.sink_item = 1000000000;
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  if (debug)
    for (size_t i = 0; i < rounds; ++i) {
      std::cout << i << " (" << input[i] << ", " << output[i] << ")"
                << std::endl;
    }

  if (!std::equal(input.begin(), input.end(), output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }
  if (!std::equal(input.begin(), input.end(), output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }
  REQUIRE(std::equal(input.begin(), input.end(), output.begin()));
}