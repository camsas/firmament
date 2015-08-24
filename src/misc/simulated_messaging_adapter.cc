// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "misc/simulated_messaging_adapter.h"

#include <string>

namespace firmament {
namespace misc {

template <typename T> void SimulatedMessagingAdapter<T>::AwaitNextMessage() {
}

template <typename T> void SimulatedMessagingAdapter<T>::CloseChannel(
    MessagingChannelInterface<T>* channel) {
  // We don't have to close the channel because we haven't established it.
}

template <typename T> bool SimulatedMessagingAdapter<T>::EstablishChannel(
    const string& endpoint_uri,
    MessagingChannelInterface<T>* channel) {
  return true;
}

template <typename T>
MessagingChannelInterface<T>*
SimulatedMessagingAdapter<T>::GetChannelForEndpoint(const string& endpoint) {
  return NULL;
}

template <typename T>
void SimulatedMessagingAdapter<T>::ListenURI(const string& endpoint_uri) {
}

template <typename T> bool SimulatedMessagingAdapter<T>::ListenReady() {
  return true;
}

template <typename T>
void SimulatedMessagingAdapter<T>::RegisterAsyncMessageReceiptCallback(
    typename AsyncMessageRecvHandler<T>::type callback) {
  // Callback not required.
}

template <typename T>
void SimulatedMessagingAdapter<T>::RegisterAsyncErrorPathCallback(
    typename AsyncErrorPathHandler<T>::type callback) {
  // We don't simulate any errors.
}

template <typename T>
bool SimulatedMessagingAdapter<T>::SendMessageToEndpoint(
    const string& endpoint_uri,
    T& message) {  // NOLINT
  return true;
}

template <class T>
ostream& SimulatedMessagingAdapter<T>::ToString(ostream* stream) const {
  return *stream << "(MessagingAdapter,type=Simulated,at=" << this << ")";
}

}  // namespace misc
}  // namespace firmament
