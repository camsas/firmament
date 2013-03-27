#ifndef THREAD_SAFE_LIMITED_QUEUE_H_INCLUDED
#define THREAD_SAFE_LIMITED_QUEUE_H_INCLUDED

#include <queue>
#include <deque>

#include <boost/thread.hpp>

namespace thread_safe {

template < class T, class Container = std::deque<T> >
class limited_queue {
public:
    explicit limited_queue( const Container & ctnr = Container() ) : storage( ctnr ), my_max_size( 0 ) { }
    bool empty( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.empty(); }

    size_t size( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.size(); }

    size_t max_size( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return my_max_size; }

    void set_max_size( size_t m ) { boost::lock_guard<boost::mutex> lock( mutex ); my_max_size = m; }

    T & back( void ) { boost::unique_lock<boost::mutex> lock( mutex ); if ( storage.empty() ) emptyCond.wait( lock ); return storage.back(); }
    const T & back( void ) const { boost::unique_lock<boost::mutex> lock( mutex ); if ( storage.empty() ) emptyCond.wait( lock ); return storage.back(); }

    T & front( void ) { boost::unique_lock<boost::mutex> lock( mutex ); if ( storage.empty() ) emptyCond.wait( lock ); return storage.front(); }
    const T & front( void ) const { boost::unique_lock<boost::mutex> lock( mutex ); if ( storage.empty() ) emptyCond.wait( lock ); return storage.front(); }

    void push( const T & u ) { boost::unique_lock<boost::mutex> lock( mutex ); if ( my_max_size != 0 && storage.size() == my_max_size ) fullCond.wait( lock ); storage.push( u ); emptyCond.notify_one(); }

    void pop( void ) { boost::unique_lock<boost::mutex> lock( mutex ); if ( storage.empty() ) emptyCond.wait( lock ); storage.pop(); fullCond.notify_one(); }
private:
    std::queue<T, Container> storage;
    mutable boost::mutex mutex;
    mutable boost::condition_variable emptyCond;
    mutable boost::condition_variable fullCond;
    size_t my_max_size;
};

}

#endif // THREAD_SAFE_LIMITED_QUEUE_H_INCLUDED
