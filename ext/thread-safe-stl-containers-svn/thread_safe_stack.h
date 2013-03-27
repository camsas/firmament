#ifndef THREAD_SAFE_STACK_H_INCLUDED
#define THREAD_SAFE_STACK_H_INCLUDED

#include <deque>

#include <boost/thread.hpp>

namespace thread_safe {

template < class T, class Container = std::deque<T> >
class stack {
public:
    explicit stack( const Container & ctnr = Container() ) : storage( ctnr ) { }
    bool empty( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.empty(); }

    size_t size( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.size(); }

    T & top( void ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage.back(); }
    const T & top( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.back(); }

    void push( const T & u ) { boost::lock_guard<boost::mutex> lock( mutex ); storage.push_back( u ); }

    void pop( void ) { boost::lock_guard<boost::mutex> lock( mutex ); storage.pop_back(); }
private:
    Container storage;
    mutable boost::mutex mutex;
};

}

#endif // THREAD_SAFE_STACK_H_INCLUDED
