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

// =========================
// 1. 日志工具
// =========================
std::mutex g_log_mutex;

template <typename... Args>
void log_line(Args&&... args) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    ((std::cout << std::forward<Args>(args)), ...) << std::endl;
}

// =========================
// 2. 基础行情结构
// =========================
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

// =========================
// 3. SPSC 无锁环形队列
// 单生产者：接收线程
// 单消费者：排序线程
// =========================
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

// =========================
// 4. 单股票排序缓冲区
// 保证单股票时间戳递增
// =========================
struct StockSortBuffer {
    std::array<MarketData, 128> buffer{};
    size_t size = 0;
    int64_t last_timestamp = -1;

    bool insert_and_sort(const MarketData& md) noexcept {
        if (size >= buffer.size()) {
            return false;
        }

        size_t i = size++;
        for (; i > 0 && buffer[i - 1].timestamp > md.timestamp; --i) {
            buffer[i] = buffer[i - 1];
        }
        buffer[i] = md;
        return true;
    }

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

// =========================
// 5. MPSC 无锁链表队列
// 多生产者：排序线程
// 单消费者：串行核心线程
// =========================
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

// =========================
// 6. 全局配置
// =========================
constexpr int RECV_THREAD_NUM = 4;
constexpr size_t PRIVATE_QUEUE_CAP = 8192;

struct RecvThreadData {
    SPSCRingBuffer<MarketData, PRIVATE_QUEUE_CAP> private_queue;
};

std::array<RecvThreadData, RECV_THREAD_NUM> recv_threads_data;
MPSCQueue<MarketData> global_mpsc_queue;
std::atomic<bool> running{true};

const std::array<std::string, 8> STOCKS = {
    "600519", "000001", "300750", "600036",
    "601318", "688981", "002415", "600276"
};

// =========================
// 7. mock 核心处理
// =========================
void calculate_factor(const MarketData& md) {
    (void)md;
}

void model_inference(const MarketData& md) {
    (void)md;
}

void strategy_logic(const MarketData& md) {
    (void)md;
}

// =========================
// 8. 接收线程：模拟行情到来
// =========================
void recv_worker(int thread_id) {
    // 启动日志
    log_line("[recv-", thread_id, "] started");

    // 每个线程自己的随机数生成器
    std::mt19937 rng(std::random_device{}());

    // 随机选择股票代码
    std::uniform_int_distribution<int> stock_dist(
        0, static_cast<int>(STOCKS.size() - 1)
    );

    // 随机生成成交量
    std::uniform_int_distribution<int> vol_dist(1, 1000);

    // 随机生成价格
    std::uniform_real_distribution<double> px_dist(10.0, 300.0);

    // 小概率制造时间戳乱序
    std::uniform_int_distribution<int> disorder_dist(0, 100);

    // 当前线程的本地时间戳
    int64_t local_ts = 0;

    // 线程内局部计数，仅用于日志输出
    uint64_t local_recv_count = 0;
    uint64_t local_drop_count = 0;

    // 主循环：只要系统还在运行，就持续生成行情
    while (running.load(std::memory_order_relaxed)) {
        MarketData md;

        // 随机选一只股票
        md.stock_code = STOCKS[stock_dist(rng)];

        // 时间推进
        local_ts += 10;

        // 少量制造乱序时间戳，模拟真实行情偶发乱序
        if (disorder_dist(rng) < 5 && local_ts > 30) {
            md.timestamp = local_ts - 20;
        } else {
            md.timestamp = local_ts;
        }

        // 随机生成价格和成交量
        md.price = px_dist(rng);
        md.volume = vol_dist(rng);

        // 写入当前线程自己的私有队列
        bool ok = recv_threads_data[thread_id].private_queue.push(md);

        if (!ok) {
            // 私有队列满，直接丢弃，避免阻塞接收线程
            ++local_drop_count;

            if (local_drop_count % 100 == 1) {
                log_line("[recv-", thread_id,
                         "] private queue full, dropped=", local_drop_count);
            }
        } else {
            // 成功写入，说明数据已进入后续处理流水线
            ++local_recv_count;

            // 降低日志频率，避免刷屏
            if (local_recv_count % 2000 == 0) {
                log_line("[recv-", thread_id, "] latest=", md);
            }
        }

        // 模拟行情到达间隔
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }

    // 退出日志
    log_line("[recv-", thread_id, "] stopped, recv=", local_recv_count,
             ", drop=", local_drop_count);
}

// =========================
// 9. 排序线程
// =========================
void sort_worker(int thread_id) {
    // 启动日志，方便确认排序线程已经启动
    log_line("[sort-", thread_id, "] started");

    // 拿到当前线程对应的私有数据

    auto& thread_data = recv_threads_data[thread_id];

    // 为“当前排序线程负责的所有股票”准备排序缓冲区
    std::unordered_map<std::string, StockSortBuffer> stock_buffers;

    // 线程内局部计数，仅用于日志节流
    uint64_t local_sort_out = 0;   // 成功输出到全局队列的条数
    uint64_t local_sort_drop = 0;  // 因缓冲区满或写全局队列失败而丢弃的条数

    // 临时变量：存放从私有队列取出的原始行情
    MarketData md;

    // 主循环：
    // 只要系统还在运行，就持续从私有队列取数据
    while (running.load(std::memory_order_relaxed)) {
        // 从“当前线程自己的私有 SPSC 队列”取一条行情
        if (thread_data.private_queue.pop(md)) {
            // 找到这只股票对应的排序缓冲区

            auto& buffer = stock_buffers[md.stock_code];

            // 把当前行情插入该股票的排序缓冲区，并按 timestamp 排序
            if (!buffer.insert_and_sort(md)) {
                // 如果插入失败，通常说明该股票的排序缓冲区满了
                ++local_sort_drop;

                // 每隔一段时间打一次日志，防止刷屏
                if (local_sort_drop % 100 == 1) {
                    log_line("[sort-", thread_id,
                             "] stock buffer full, stock=", md.stock_code,
                             ", dropped=", local_sort_drop);
                }
                continue;
            }

            // 尝试不断从该股票缓冲区中取出“已经满足顺序要求”的行情
            MarketData ordered_md;
            while (buffer.pop_ordered(ordered_md)) {
                // 把排好序的行情送入全局 MPSC 队列
                if (global_mpsc_queue.push(ordered_md)) {
                    ++local_sort_out;

                    // 降低日志频率，避免刷屏
                    if (local_sort_out % 2000 == 0) {
                        log_line("[sort-", thread_id, "] ordered out=", ordered_md);
                    }
                } else {
                    // 理论上这里一般不太会失败，除非内存分配失败等
                    ++local_sort_drop;

                    if (local_sort_drop % 100 == 1) {
                        log_line("[sort-", thread_id,
                                 "] global MPSC push failed, dropped=", local_sort_drop);
                    }
                }
            }
        } else {
            // 私有队列暂时没数据，做一个轻量级 pause，自旋等待
            // 这样不会像 sleep 那样切走线程，但也能稍微降低空转开销
            _mm_pause();
        }
    }

    // 退出前做一次“收尾”：
    // 把私有队列里还没来得及处理的数据尽量清掉，避免直接丢失
    while (thread_data.private_queue.pop(md)) {
        auto& buffer = stock_buffers[md.stock_code];

        if (!buffer.insert_and_sort(md)) {
            continue;
        }

        MarketData ordered_md;
        while (buffer.pop_ordered(ordered_md)) {
            global_mpsc_queue.push(ordered_md);
        }
    }

    // 线程退出日志
    log_line("[sort-", thread_id, "] stopped, out=", local_sort_out,
             ", drop=", local_sort_drop);
}

// =========================
// 10. 串行核心线程
// =========================
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
                log_line("[core] consumed=", consume_count,
                         ", queue_size=", global_mpsc_queue.size(),
                         ", latest=", md);
            }
        }
        _mm_pause();
    }

    log_line("[core] stopped, total consumed=", consume_count);
}

// =========================
// 11. main
// =========================
int main() {
    log_line("[main] HFT pipeline demo started");

    std::vector<std::thread> recv_workers;
    std::vector<std::thread> sort_workers;
    std::thread core_thread(serial_core_worker);

    for (int i = 0; i < RECV_THREAD_NUM; ++i) {
        recv_workers.emplace_back(recv_worker, i);
    }

    for (int i = 0; i < RECV_THREAD_NUM; ++i) {
        sort_workers.emplace_back(sort_worker, i);
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
    running.store(false, std::memory_order_relaxed);

    for (auto& t : recv_workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    for (auto& t : sort_workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    if (core_thread.joinable()) {
        core_thread.join();
    }

    log_line("[main] done");
    return 0;
}