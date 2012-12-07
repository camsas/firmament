/* 
 * File:   types.h
 * Author: nscc2
 *
 * Created on 04 December 2012, 14:41
 */

#ifndef TYPES_H
#define	TYPES_H

#include "storage/reference_interface.h"
#include "boost/bimap.hpp"
#include <boost/bimap/list_of.hpp> 
#include <boost/bimap/set_of.hpp> 

namespace firmament { 
namespace store { 
    
    typedef map<DataObjectID_t, ReferenceDescriptor> DataObjectMap_t;
    
    typedef boost::bimaps::bimap<boost::bimaps::set_of<DataObjectID_t>, 
            boost::bimaps::list_of<pair<void*, size_t > Cache_t; 
    //TACH: TODO not the best way to store size with object
}
}

#endif	/* TYPES_H */

