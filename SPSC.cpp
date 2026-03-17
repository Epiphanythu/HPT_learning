#include <iostream>
#include <thread>
#include <atomic>
#include <array>
#include <chrono>

template<typename T, size_t CAPACITY>
class SPSCRingBuffer {
private:

    alignas(64) std::array<T, CAPACITY> buffer;

    alignas(64) std::atomic<size_t> write_index{0};

    alignas(64) std::atomic<size_t> read_index{0};

public:

    bool push(const T& value) {

        size_t w = write_index.load(std::memory_order_relaxed); // 只保证原子，不保证顺序

        size_t next = (w + 1) % CAPACITY;

        if (next == read_index.load(std::memory_order_acquire)) {
            return false; // 队列满
        }

        buffer[w] = value;

        write_index.store(next, std::memory_order_release); // 先写数据再更新 

        return true;
    }

    bool pop(T& value) {

        size_t r = read_index.load(std::memory_order_relaxed);

        if (r == write_index.load(std::memory_order_acquire)) {
            return false; // 队列空
        }

        value = buffer[r];

        read_index.store((r + 1) % CAPACITY, std::memory_order_release);

        return true;
    }
};

SPSCRingBuffer<int, 1024> ringbuffer;

void producer(std::stop_token st)
{
    int value = 0;

    while (!st.stop_requested())
    {
        if (ringbuffer.push(value))
        {
            std::cout << "Produced: " << value << std::endl;
            value++;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "Producer stopped\n";
}


void consumer(std::stop_token st)
{
    int value;

    while (!st.stop_requested())
    {
        if (ringbuffer.pop(value))
        {
            std::cout << "Consumed: " << value << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    std::cout << "Consumer stopped\n";
}


int main()
{
    std::jthread producer_thread(producer);

    std::jthread consumer_thread(consumer);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    producer_thread.request_stop();
    consumer_thread.request_stop();

    return 0;
}