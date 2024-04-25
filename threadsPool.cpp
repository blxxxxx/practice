#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>
#include <functional>
#include <future>
#include <queue>
#include <shared_mutex>

template<typename T>
class Thread_Safe_Queue {
private:
    std::queue<T> Q;
    std::shared_mutex m;
public:
    bool empty() {
        std::shared_lock<std::shared_mutex> lk{m};
        return Q.empty();
    }
    size_t size() {
        std::shared_lock<std::shared_mutex> lk{m};
        return Q.size();
    }
    void push(T& x) {
        std::unique_lock<std::shared_mutex> lk{m};
        Q.push(std::move(x));
    }
    bool pop(T& x) {
        std::unique_lock<std::shared_mutex> lk{m};
        if(Q.empty())return false;
        x = std::move(Q.front());
        Q.pop();
        return true;
    }
};

class Threads_Pool {
private:
    std::vector<std::thread> threads;
    bool is_shut_down;
    std::mutex m_;
    std::condition_variable cv;
    Thread_Safe_Queue<std::function<void()>> taskQ;
    struct worker {
        Threads_Pool* pool;
        explicit worker(Threads_Pool* p) : pool(p) {}
        void operator()() const{
            while(pool->is_shut_down == false || !pool->taskQ.empty()) {
                {
                    std::unique_lock<std::mutex> lk{pool->m_};
                    pool->cv.wait(lk,[this]() {
                        if(pool->is_shut_down == true)return true;
                        return !pool->taskQ.empty();
                    });
                }
                std::function<void()> f;
                bool flag = pool->taskQ.pop(f);
                if(flag) {
                    f();
                }
            }
        }
    };
public:
    explicit Threads_Pool(const int n) : is_shut_down(false),threads(n){
        for(auto &t : threads) {
            t = std::thread{ worker(this) };
        }
    }
    template <typename F,typename... Args>
    auto submit(F&& func,Args&&... args) -> std::future<decltype(func(args...))>{
        using return_t = decltype(func(args...));
        auto f_bind = std::bind(func,args...);
        auto task_ptr = std::make_shared<std::packaged_task<return_t()>>(f_bind);
        std::function<void()> void_F = [task_ptr]() {
            (*task_ptr)();
        };
        taskQ.push(void_F);
        cv.notify_one();
        return task_ptr->get_future();
    }
    ~Threads_Pool() {
        is_shut_down = true;
        cv.notify_all();
        for(auto &t : threads) {
            if(t.joinable())t.join();
        }
    }
    Threads_Pool() = delete;
    Threads_Pool(const Threads_Pool& x) = delete;
    Threads_Pool(const Threads_Pool&& x) = delete;
    Threads_Pool operator=(const Threads_Pool& x) = delete;
    Threads_Pool operator=(Threads_Pool x) = delete;
};
using namespace std::chrono_literals;
std::mutex m;

void f(int id) {
    std::unique_lock<std::mutex> lk{m};
    std::cout << "id : " << id << std::endl;
}
int ksm(int a,int b) {
    std::this_thread::sleep_for(5s);
    int p = 998244353;
    int res = 1;
    while(b) {
        if(b & 1)res = 1ll * res * a % p;
        a = a * a % p;
        b >>= 1;
    }
    return res;
}
int main() {
    Threads_Pool pool(3);
    int n = 20;
    std::future<int> ksm_res = pool.submit(ksm,2,10);
    // ksm_res.wait();
    for (int i = 1; i <= n; i++) {
        // pool.submit(f,i);
        pool.submit(f,i);
    }
    std::cout << ksm_res.get() << '\n';
    return 0;
}