#pragma once

/*
 * is boost::property_tree::ptree thread safe?
 * ===========================================
 * https://stackoverflow.com/questions/8156948/is-boostproperty-treeptree-thread-safe
 *
 * Because boost json parser depends on boost::spirit, and spirit is not
 * thread-safety default. You can add this macro before any ptree header file
 * to resolve it.
 */

#define BOOST_SPIRIT_THREADSAFE
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/exceptions.hpp>