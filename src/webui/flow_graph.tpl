{{>HEADER}}

{{>PAGE_HEADER}}

<style type="text/css">
  #flow-graph {
    width: 100%;
    min-height: 90%;
    border: 1px solid lightgray;
  }
</style>

<h1>Current flow graph</h1>

<div id="flow-graph"></div>

<script type="text/javascript">
// create an array with nodes
var nodes;
// create an array with edges
var edges;

$(function() {
  url = "/sched/flowgraph/?json=1";
  $.ajax({
    url: url,
    async: false,
    dataType: 'json',
    success: function(data) {
      nodes = new vis.DataSet(data.nodes);
      edges = new vis.DataSet(data.edges);
      plotNetwork();
    },
    error: function(req, err, et) {
      window.alert(et);
    }});
});

function plotNetwork() {
  // create a network
  var container = document.getElementById('flow-graph');
  var data = {
    nodes: nodes,
    edges: edges
  };
  var options = {
    layout: {
    }, 
    edges: {
      smooth: true,
      arrows: {to : true }
    }
  };
  var network = new vis.Network(container, data, options);
}
</script>


{{>PAGE_FOOTER}}
