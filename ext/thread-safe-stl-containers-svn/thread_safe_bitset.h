#ifndef THREAD_SAFE_BITSET_H_INCLUDED
#define THREAD_SAFE_BITSET_H_INCLUDED

#include <iostream>
#include <bitset>
#include <string>
#include <boost/thread.hpp>

namespace thread_safe {

template <size_t N>
class bitset {
    template <size_t U> friend thread_safe::bitset<U> operator& (const thread_safe::bitset<U>& lhs, const thread_safe::bitset<U>& rhs);
    template <size_t U> friend thread_safe::bitset<U> operator| (const thread_safe::bitset<U>& lhs, const thread_safe::bitset<U>& rhs);
    template <size_t U> friend thread_safe::bitset<U> operator^ (const thread_safe::bitset<U>& lhs, const thread_safe::bitset<U>& rhs);

    template <class charT, class traits, size_t U> friend std::basic_istream<charT, traits> & operator>> ( std::basic_istream<charT,traits>& is, thread_safe::bitset<U>& rhs);
    template <class charT, class traits, size_t U> friend std::basic_ostream<charT, traits> & operator<< ( std::basic_ostream<charT,traits>& os, const thread_safe::bitset<U>& rhs);
public:
    //Constructors
    bitset( void ) { }
    bitset( unsigned long val ) : storage( val ) { }
    template< class charT, class traits, class Allocator>
    explicit bitset( const std::basic_string<charT, traits,Allocator>& str,
            typename std::basic_string<charT,traits,Allocator>::size_type pos = 0,
            typename std::basic_string<charT,traits,Allocator>::size_type n = std::basic_string<charT,traits,Allocator>::npos ) : storage( str, pos, n ) { }

    //Bit Access
    bool operator[]( size_t pos ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage[pos]; }
    bool & operator[]( size_t pos ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage[pos]; }

    //Bitset operators
    thread_safe::bitset<N> & operator&=( const thread_safe::bitset<N> & rhs ) { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); storage &= rhs.storage; return *this; }
    thread_safe::bitset<N> & operator|=( const thread_safe::bitset<N> & rhs ) { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); storage |= rhs.storage; return *this; }
    thread_safe::bitset<N> & operator^=( const thread_safe::bitset<N> & rhs ) { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); storage ^= rhs.storage; return *this; }
    thread_safe::bitset<N> & operator<<=( const thread_safe::bitset<N> & rhs ) { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); storage <<= rhs.storage; return *this; }
    thread_safe::bitset<N> & operator>>=( const thread_safe::bitset<N> & rhs ) { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); storage >>= rhs.storage; return *this; }
    thread_safe::bitset<N> operator~( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); bitset<N> temp( *this ); temp.storage = ~temp.storage; return temp; }
    thread_safe::bitset<N> operator<<( size_t pos ) const { boost::lock_guard<boost::mutex> lock( mutex ); bitset<N> temp( *this ); temp.storage << pos; return temp; }
    thread_safe::bitset<N> operator>>( size_t pos ) const { boost::lock_guard<boost::mutex> lock( mutex ); bitset<N> temp( *this ); temp.storage >> pos; return temp; }
    bool operator==( const thread_safe::bitset<N>& rhs ) const { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); return storage == rhs.storage; }
    bool operator!=( const thread_safe::bitset<N>& rhs ) const { boost::lock_guard<boost::mutex> lock( mutex ); boost::lock_guard<boost::mutex> lock2( rhs.mutex ); return storage != rhs.storage; }

    //Bit operations
    thread_safe::bitset<N> & set( void ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage.set(); }
    thread_safe::bitset<N> & set( size_t pos, bool val = true ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage.set( pos, true ); }

    thread_safe::bitset<N> & flip( void ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage.flip(); }
    thread_safe::bitset<N> & flip( size_t pos ) { boost::lock_guard<boost::mutex> lock( mutex ); return storage.flip( pos ); }

    //Bitset operations
    unsigned long to_ulong( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.to_ulong(); }

    template < class charT, class traits, class Allocator>
        std::basic_string<charT, traits, Allocator> to_string( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.to_string(); }

    size_t count( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.count(); }

    size_t size( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.size(); }

    bool test( size_t pos ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.test( pos ); }

    bool any( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.any(); }

    bool none( void ) const { boost::lock_guard<boost::mutex> lock( mutex ); return storage.none(); }

private:
    std::bitset<N> storage;
    mutable boost::mutex mutex;
};

template<size_t N>
thread_safe::bitset<N> operator& (const thread_safe::bitset<N>& lhs, const thread_safe::bitset<N>& rhs) {
    boost::lock_guard<boost::mutex> lock( lhs.mutex );
    boost::lock_guard<boost::mutex> lock2( rhs.mutex );
    bitset<N> temp;
    temp.storage = lhs.storage & rhs.storage;
    return temp;
}

template<size_t N>
thread_safe::bitset<N> operator| (const thread_safe::bitset<N>& lhs, const thread_safe::bitset<N>& rhs) {
    boost::lock_guard<boost::mutex> lock( lhs.mutex );
    boost::lock_guard<boost::mutex> lock2( rhs.mutex );
    bitset<N> temp;
    temp.storage = lhs.storage | rhs.storage;
    return temp;
}

template<size_t N>
thread_safe::bitset<N> operator^ (const thread_safe::bitset<N>& lhs, const thread_safe::bitset<N>& rhs) {
    boost::lock_guard<boost::mutex> lock( lhs.mutex );
    boost::lock_guard<boost::mutex> lock2( rhs.mutex );
    bitset<N> temp;
    temp.storage = lhs.storage ^ rhs.storage;
    return temp;
}

template <class charT, class traits, size_t N>
std::basic_istream<charT, traits> & operator>> ( std::basic_istream<charT,traits>& is, thread_safe::bitset<N>& rhs) {
    boost::lock_guard<boost::mutex> lock2( rhs.mutex );
    return is >> rhs.storage;
}

template <class charT, class traits, size_t N>
std::basic_ostream<charT, traits> & operator<< ( std::basic_ostream<charT,traits>& os, const thread_safe::bitset<N>& rhs) {
    boost::lock_guard<boost::mutex> lock2( rhs.mutex );
    return os << rhs.storage;
}

}

#endif // THREAD_SAFE_BITSET_H_INCLUDED
