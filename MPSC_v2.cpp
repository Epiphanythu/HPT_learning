#include <atomic>
#include <array>
#include <chrono>
#include <cstdint>
#include <immintrin.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// 日志工具
std::mutex g_log_mutex;

template <typename... Args>
void log_line(Args&&... args) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    ((std::cout << std::forward<Args>(args)), ...) << std::endl;
}

// 基础行情结构
struct MarketData {
    std::string stock_code;
    int64_t timestamp = 0;
    double price = 0.0;
    int volume = 0;
};

std::ostream& operator<<(std::ostream& os, const MarketData& md) {
    os << "{stock=" << md.stock_code
       << ", ts=" << md.timestamp
       << ", price=" << std::fixed << std::setprecision(2) << md.price
       << ", vol=" << md.volume
       << "}";
    return os;
}

// SPSC 无锁环形队列
// 单生产者：接收线程
// 单消费者：排序线程
template <typename T, size_t CAPACITY>
class SPSCRingBuffer {
    static_assert(CAPACITY >= 2, "CAPACITY must be >= 2");

private:
    alignas(64) std::array<T, CAPACITY> buffer_{};
    alignas(64) std::atomic<size_t> write_idx_{0};
    alignas(64) std::atomic<size_t> read_idx_{0};

public:
    bool push(const T& data) noexcept {
        size_t curr_write = write_idx_.load(std::memory_order_relaxed);
        size_t next_write = (curr_write + 1) % CAPACITY;

        if (next_write == read_idx_.load(std::memory_order_acquire)) {
            return false;
        }

        buffer_[curr_write] = data;
        write_idx_.store(next_write, std::memory_order_release);
        return true;
    }

    bool pop(T& data) noexcept {
        size_t curr_read = read_idx_.load(std::memory_order_relaxed);

        if (curr_read == write_idx_.load(std::memory_order_acquire)) {
            return false;
        }

        data = buffer_[curr_read];
        read_idx_.store((curr_read + 1) % CAPACITY, std::memory_order_release);
        return true;
    }
};

// 单股票排序缓冲区
// 保证单股票时间戳递增
struct StockSortBuffer {
    std::array<MarketData, 1024> buffer{};
    size_t size = 0;
    int64_t last_timestamp = -1;

    // 插入行情并按时间戳排序
    bool insert_and_sort(const MarketData& md) noexcept {
        if (size >= buffer.size()) {
            // 缓冲区满，强制丢弃最旧的数据（通常是过期的乱序数据）
            for (size_t i = 1; i < size; ++i) {
                buffer[i - 1] = buffer[i];
            }
            --size;
        }

        size_t i = size++;
        for (; i > 0 && buffer[i - 1].timestamp > md.timestamp; --i) {
            buffer[i] = buffer[i - 1];
        }
        buffer[i] = md;
        return true;
    }

    // 输出有序行情（仅输出比 last_timestamp 大的）
    bool pop_ordered(MarketData& md) noexcept {
        if (size == 0) {
            return false;
        }

        if (buffer[0].timestamp > last_timestamp) {
            md = buffer[0];
            last_timestamp = md.timestamp;

            for (size_t i = 1; i < size; ++i) {
                buffer[i - 1] = buffer[i];
            }
            --size;
            return true;
        }

        return false;
    }
};

// MPSC 无锁链表队列
// 多生产者：排序线程
// 单消费者：串行核心线程
template <typename T>
class MPSCQueue {
private:
    struct Node {
        T data{};
        std::atomic<Node*> next{nullptr};

        Node() = default;
        explicit Node(const T& d) : data(d), next(nullptr) {}
    };

    alignas(64) std::atomic<Node*> head_{nullptr};
    alignas(64) std::atomic<Node*> tail_{nullptr};
    std::atomic<size_t> size_{0};

public:
    MPSCQueue() {
        Node* dummy = new Node();
        head_.store(dummy, std::memory_order_relaxed);
        tail_.store(dummy, std::memory_order_relaxed);
    }

    ~MPSCQueue() {
        Node* node = head_.load(std::memory_order_relaxed);
        while (node) {
            Node* next = node->next.load(std::memory_order_relaxed);
            delete node;
            node = next;
        }
    }

    // 多生产者入队（无锁）
    bool push(const T& data) noexcept {
        Node* new_node = new (std::nothrow) Node(data);
        if (!new_node) {
            return false;
        }

        Node* old_tail = tail_.load(std::memory_order_acquire);
        while (!tail_.compare_exchange_weak(
            old_tail,
            new_node,
            std::memory_order_release,
            std::memory_order_relaxed)) {
        }

        old_tail->next.store(new_node, std::memory_order_release);
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // 单消费者出队（无锁）
    bool pop(T& data) noexcept {
        Node* old_head = head_.load(std::memory_order_acquire);
        Node* next = old_head->next.load(std::memory_order_acquire);

        if (!next) {
            return false;
        }

        data = next->data;
        head_.store(next, std::memory_order_release);
        delete old_head;
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }

    size_t size() const noexcept {
        return size_.load(std::memory_order_relaxed);
    }
};

// 全局配置
constexpr int RECV_THREAD_NUM = 4;      // 接收线程数
constexpr int SORT_THREAD_NUM = 4;      // 排序线程数
constexpr size_t PRIVATE_QUEUE_CAP = 8192;

// 每个排序线程有自己独立的接收线程组
// 通过股票代码哈希，确保同一股票的行情永远进入同一个排序线程
struct SortThreadData {
    std::array<SPSCRingBuffer<MarketData, PRIVATE_QUEUE_CAP>, RECV_THREAD_NUM> recv_queues;
    std::unordered_map<std::string, StockSortBuffer> stock_buffers;
};

std::array<SortThreadData, SORT_THREAD_NUM> sort_threads_data;
MPSCQueue<MarketData> global_mpsc_queue;
std::atomic<bool> running{true};

// 全局原子时间戳：确保所有接收线程生成的时间戳全局递增、不重复
alignas(64) std::atomic<int64_t> global_timestamp{0};

const std::array<std::string, 8> STOCKS = {
    "600519", "000001", "300750", "600036",
    "601318", "688981", "002415", "600276"
};

// 股票代码哈希函数
// 确保同一股票永远路由到同一个排序线程
inline int get_sort_thread_id(const std::string& stock_code) {
    return static_cast<int>(std::hash<std::string>{}(stock_code) % SORT_THREAD_NUM);
}

// mock 核心处理
void calculate_factor(const MarketData& md) { (void)md; }
void model_inference(const MarketData& md) { (void)md; }
void strategy_logic(const MarketData& md) { (void)md; }

// 接收线程：模拟行情到来
// 关键改进：按股票代码哈希路由到对应排序线程
void recv_worker(int thread_id) {
    log_line("[recv-", thread_id, "] started");

    std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> stock_dist(0, static_cast<int>(STOCKS.size() - 1));
    std::uniform_int_distribution<int> vol_dist(1, 1000);
    std::uniform_real_distribution<double> px_dist(10.0, 300.0);
    std::uniform_int_distribution<int> disorder_dist(0, 100);

    uint64_t local_recv_count = 0;
    uint64_t local_drop_count = 0;

    while (running.load(std::memory_order_relaxed)) {
        MarketData md;

        // 随机选一只股票
        md.stock_code = STOCKS[stock_dist(rng)];

        // 使用全局原子时间戳，确保跨线程递增且不重复
        int64_t ts = global_timestamp.fetch_add(10, std::memory_order_relaxed);

        // 5% 概率制造小范围乱序（回退1-3，模拟真实网络微延迟）
        if (disorder_dist(rng) < 5 && ts > 10) {
            md.timestamp = ts - (1 + disorder_dist(rng) % 3);
        } else {
            md.timestamp = ts;
        }

        md.price = px_dist(rng);
        md.volume = vol_dist(rng);

        // 关键：按股票代码哈希，路由到对应的排序线程
        int sort_id = get_sort_thread_id(md.stock_code);

        // 写入对应排序线程的私有队列
        bool ok = sort_threads_data[sort_id].recv_queues[thread_id].push(md);

        if (!ok) {
            ++local_drop_count;
            if (local_drop_count % 100 == 1) {
                log_line("[recv-", thread_id, "] queue full, dropped=", local_drop_count);
            }
        } else {
            ++local_recv_count;
            if (local_recv_count % 2000 == 0) {
                log_line("[recv-", thread_id, "] latest=", md);
            }
        }

        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

    log_line("[recv-", thread_id, "] stopped, recv=", local_recv_count, ", drop=", local_drop_count);
}

// 排序线程
// 关键：同一股票的所有行情都会路由到同一个排序线程
// 因此 last_timestamp 检查不会出现跨线程冲突
void sort_worker(int sort_id) {
    log_line("[sort-", sort_id, "] started");

    auto& thread_data = sort_threads_data[sort_id];
    uint64_t local_sort_out = 0;
    uint64_t local_sort_drop = 0;
    MarketData md;

    while (running.load(std::memory_order_relaxed)) {
        bool has_data = false;

        // 轮询所有接收线程的队列
        for (int recv_id = 0; recv_id < RECV_THREAD_NUM; ++recv_id) {
            if (thread_data.recv_queues[recv_id].pop(md)) {
                has_data = true;

                // 找到该股票的排序缓冲区
                auto& buffer = thread_data.stock_buffers[md.stock_code];

                if (!buffer.insert_and_sort(md)) {
                    ++local_sort_drop;
                    continue;
                }

                // 尝试输出有序行情
                MarketData ordered_md;
                while (buffer.pop_ordered(ordered_md)) {
                    if (global_mpsc_queue.push(ordered_md)) {
                        ++local_sort_out;
                        if (local_sort_out % 2000 == 0) {
                            log_line("[sort-", sort_id, "] ordered out=", ordered_md);
                        }
                    } else {
                        ++local_sort_drop;
                    }
                }
            }
        }

        if (!has_data) {
            _mm_pause();
        }
    }

    // 收尾：处理剩余数据
    for (int recv_id = 0; recv_id < RECV_THREAD_NUM; ++recv_id) {
        while (thread_data.recv_queues[recv_id].pop(md)) {
            auto& buffer = thread_data.stock_buffers[md.stock_code];
            if (buffer.insert_and_sort(md)) {
                MarketData ordered_md;
                while (buffer.pop_ordered(ordered_md)) {
                    global_mpsc_queue.push(ordered_md);
                }
            }
        }
    }

    log_line("[sort-", sort_id, "] stopped, out=", local_sort_out, ", drop=", local_sort_drop);
}

// 串行核心线程
void serial_core_worker() {
    log_line("[core] started");

    MarketData md;
    uint64_t consume_count = 0;

    while (running.load(std::memory_order_relaxed) || global_mpsc_queue.size() > 0) {
        while (global_mpsc_queue.pop(md)) {
            calculate_factor(md);
            model_inference(md);
            strategy_logic(md);

            ++consume_count;
            if (consume_count % 3000 == 0) {
                log_line("[core] consumed=", consume_count, ", queue_size=", global_mpsc_queue.size());
            }
        }
        _mm_pause();
    }

    log_line("[core] stopped, total consumed=", consume_count);
}

int main() {
    log_line("[main] HFT pipeline started");

    std::vector<std::thread> recv_workers;
    std::vector<std::thread> sort_workers;
    std::thread core_thread(serial_core_worker);

    // 启动排序线程
    for (int i = 0; i < SORT_THREAD_NUM; ++i) {
        sort_workers.emplace_back(sort_worker, i);
    }

    // 启动接收线程
    for (int i = 0; i < RECV_THREAD_NUM; ++i) {
        recv_workers.emplace_back(recv_worker, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    running.store(false, std::memory_order_relaxed);

    for (auto& t : recv_workers) {
        if (t.joinable()) t.join();
    }

    for (auto& t : sort_workers) {
        if (t.joinable()) t.join();
    }

    if (core_thread.joinable()) core_thread.join();

    log_line("[main] done");
    return 0;
}
