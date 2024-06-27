#include <iostream>
#include <fstream>
#include <cstdint>
#include <climits>
#include <cinttypes>
#include <vector>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

using namespace std;
mutex mtx;

struct Result {
    uint64_t total_sum;
    uint32_t min_num;
    uint32_t max_num;
};

void analyze_binary_file(const string& file_name, uint64_t& total_sum, uint32_t& min_num, uint32_t& max_num) {
    total_sum = 0;
    min_num = UINT32_MAX;
    max_num = 0;

    ifstream file(file_name, ios::binary);
    if (!file) {
        cerr << "Не удалось открыть файл для чтения\n";
        return;
    }

    uint32_t num;
    while (file.read(reinterpret_cast<char*>(&num), sizeof(num))) {
        uint32_t num_be = ntohl(num);  // Перевод из big endian
        total_sum += num_be;
        if (num_be < min_num) min_num = num_be;
        if (num_be > max_num) max_num = num_be;
    }

    file.close();
}


void process_chunk(int fd, size_t start, size_t size, Result &result) {
    void* addr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, start);
    if (addr == MAP_FAILED) {
        cerr << "Не удалось создать отображение памяти\n";
        return;
    }

    uint32_t* data = static_cast<uint32_t*>(addr);

    result.total_sum = 0;
    result.min_num = UINT32_MAX;
    result.max_num = 0;

    size_t count = size / 4;
    for (size_t i = 0; i < count; ++i) {
        uint32_t num = ntohl(data[i]);
        result.total_sum += num;
        if (num < result.min_num) result.min_num = num;
        if (num > result.max_num) result.max_num = num;
    }

    munmap(addr, size);
}

void analyze_binary_file_multithreaded(const string& file_name, size_t num_threads, uint64_t& total_sum, uint32_t& min_num, uint32_t& max_num) {
    int fd = open(file_name.c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Не удалось открыть файл для чтения\n";
        return;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    size_t chunk_size = file_size / num_threads;

    vector<thread> threads;
    vector<Result> results(num_threads);

    for (size_t i = 0; i < num_threads; ++i) {
        size_t start = i * chunk_size;
        size_t size = (i != num_threads - 1) ? chunk_size : file_size - start;
        threads.emplace_back(process_chunk,

fd, start, size, ref(results[i]));
    }

    for (auto &t : threads) {
        if (t.joinable()) t.join();
    }

    close(fd);

    total_sum = 0;
    min_num = UINT32_MAX;
    max_num = 0;
    for (const auto &result : results) {
        total_sum += result.total_sum;
        if (result.min_num < min_num) min_num = result.min_num;
        if (result.max_num > max_num) max_num = result.max_num;
    }
}

int main() {
    uint64_t total_sum;
    uint32_t min_num, max_num;

    double start_time = time(nullptr);
    analyze_binary_file("random_numbers.bin", total_sum, min_num, max_num);
    double end_time = time(nullptr);
    cout << "Последовательное чтение заняло " << (end_time - start_time) << " секунд\n";
    cout << "Сумма: " << total_sum << ", Минимум: " << min_num << ", Максимум: " << max_num << '\n';

    start_time = time(nullptr);
    analyze_binary_file_multithreaded("random_numbers.bin", 8, total_sum, min_num, max_num);
    end_time = time(nullptr);
    cout << "Многопоточное чтение заняло " << (end_time - start_time) << " секунд\n";
    cout << "Сумма: " << total_sum << ", Минимум: " << min_num << ", Максимум: " << max_num << '\n';

    return 0;
}
