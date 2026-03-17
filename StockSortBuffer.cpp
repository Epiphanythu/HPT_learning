#include <iostream>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <unordered_map>
#include <array>
#include <chrono>
#include <random>

// ===== 模拟行情数据 =====
struct MarketData {
    std::string stock_code;
    int64_t timestamp;
};

// ===== 简化队列（为了演示，用mutex代替SPSC）=====
std::queue<MarketData> private_queue;
std::mutex queue_mutex;

// ===== 单股票排序缓冲区 =====
struct StockSortBuffer {
    std::array<MarketData, 128> buffer;
    size_t size = 0;
    int64_t last_timestamp = 0;

    bool insert_and_sort(const MarketData& md) {
        if (size >= buffer.size()) return false;

        size_t i = size++;
        for (; i > 0 && buffer[i - 1].timestamp > md.timestamp; --i) {
            buffer[i] = buffer[i - 1];
        }
        buffer[i] = md;
        return true;
    }

    bool pop_ordered(MarketData& md) {
        if (size == 0) return false;

        if (buffer[0].timestamp > last_timestamp) {
            md = buffer[0];
            last_timestamp = md.timestamp;

            for (size_t i = 1; i < size; ++i) {
                buffer[i - 1] = buffer[i];
            }
            size--;
            return true;
        }
        return false;
    }
};

// ===== 模拟接收线程（产生乱序数据）=====
void producer() {
    std::vector<int> timestamps = {1002, 1001, 1003, 1005, 1004};

    for (auto t : timestamps) {
        MarketData md{"AAPL", t};

        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            private_queue.push(md);
        }

        std::cout << "[接收线程] 收到行情: " << t << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

// ===== 排序线程（核心！）=====
void sort_worker() {

    std::unordered_map<std::string, StockSortBuffer> stock_buffers;

    while (true) {

        MarketData md;
        bool has_data = false;

        // 从队列取数据
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (!private_queue.empty()) {
                md = private_queue.front();
                private_queue.pop();
                has_data = true;
            }
        }

        if (has_data) {

            // 找到对应股票buffer
            auto& buffer = stock_buffers[md.stock_code];

            // 插入排序
            buffer.insert_and_sort(md);

            // 输出有序数据
            MarketData ordered;
            while (buffer.pop_ordered(ordered)) {
                std::cout << ">>> 输出有序行情: " << ordered.timestamp << std::endl;
            }

        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
}

// ===== 主函数 =====
int main() {

    std::thread t1(producer);      // 接收线程
    std::thread t2(sort_worker);   // 排序线程

    t1.join();

    // 让排序线程多跑一会儿
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 强制退出（demo简单处理）
    exit(0);
}