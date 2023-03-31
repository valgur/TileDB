


#ifdef TILEDB_DAG_NODES_DETAIL_RESUMABLE_PROXY_H
#define TILEDB_DAG_NODES_DETAIL_RESUMABLE_PROXY_H


template <class MimoNode, size_t portnum>
struct Proxy {
  constexpr static const size_t portnum_{portnum};
  // @todo: Proxy doesn't have any particular port it applies to, so we don't
  // know whether it's
  //   proxying an input or an output
  // using in_value_type = typename std::tuple_element_t<portnum, typename
  // MimoNode::in_value_type>; using out_value_type = typename
  // std::tuple_element_t<portnum, typename MimoNode::out_value_type>;
  MimoNode* node_ptr_;
  Proxy(MimoNode& node)
      : node_ptr_{&node} {
  }
};

template <size_t N, class T>
auto make_proxy(const T& u) {
  return Proxy<std::remove_reference_t<decltype(u)>, N>(u);
}
template <typename T>
struct is_proxy : std::false_type {};

template <typename T, size_t portnum>
struct is_proxy<Proxy<T, portnum>> : std::true_type {};

template <class T>
constexpr const bool is_proxy_v{is_proxy<T>::value};

#if 0
/**
 * @brief A proxy class for accessing the inputs and outputs of a mimo node
 * as if they were a single port.
 *
 * @todo Should be able to unify all of these into a single class, and use
 * ctad -- leave that for later
 *
 * @tparam MimoNode The mimo node type
 * @tparam portnum The port number to access
 */
template <class MimoNode, size_t input_portnum, size_t output_portnum>
struct Proxy : public std::tuple_element<input_portnum, decltype(MimoNode::inputs_)>::type,
               public std::tuple_element<output_portnum, decltype(MimoNode::outputs_)>::type {
  using input_ = typename std::tuple_element<input_portnum, decltype(MimoNode::inputs_)>::type;
  using output_ = typename std::tuple_element<output_portnum, decltype(MimoNode::outputs_)>::type;

  /**
   * @brief Construct a new Proxy object
   *
   * @param node The mimo node to proxy
   */
  Proxy(MimoNode& node) : input_{std::get<input_portnum>(node.inputs_)},
      output_{std::get<output_portnum>(node.outputs_)}{}
};

/**
 * @brief A proxy class for accessing the inputs of a mimo node
 * as if they were a single port.
 *
 * @tparam MimoNode The mimo node type
 * @tparam portnum The port number to access
 */
template <class MimoNode, size_t portnum>
struct InputProxy : public std::tuple_element<portnum, decltype(MimoNode::inputs_)>::type {
  using input_ = typename std::tuple_element<portnum, decltype(MimoNode::inputs_)>::type;

  /**
   * @brief Construct a new InputProxy object
   *
   * @param node The mimo node to proxy
   */
  InputProxy(MimoNode& node) : input_{std::get<portnum>(node.inputs_)}{}
};

/**
 * @brief A proxy class for accessing the outputs of a mimo node
 * @tparam MimoNode
 * @tparam portnum
 */
template <class MimoNode, size_t portnum>
struct OutputProxy : public std::tuple_element<portnum, decltype(MimoNode::outputs_)>::type {
  using output_ = typename std::tuple_element<portnum, decltype(MimoNode::outputs_)>::type;

  /**
   * @brief Construct a new OutputProxy object
   *
   * @param node The mimo node to proxy
   */
  OutputProxy(MimoNode& node) : output_{std::get<portnum>(node.outputs_)}{}
};
#endif

#endif // TILEDB_DAG_NODES_DETAIL_RESUMABLE_PROXY_H