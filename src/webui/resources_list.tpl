{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Resources</h1>

<h2>Overview</h2>

<table border="1">
  <tr>
    <th>#</th>
    <th>Resource ID</th>
    <th>Friendly name</th>
    <th>State</th>
    <th>Options</th>
  </tr>
  {{#RES_DATA}}
  <tr {{#RES_NON_SCHEDULABLE}}style="background-color: lightgray;"{{/RES_NON_SCHEDULABLE}}>
    <td>{{RES_NUM}}</td>
    <td>{{RES_ID}}</td>
    <td>{{RES_FRIENDLY_NAME}}</td>
    <td>{{RES_STATE}}</td>
    <td>
      <a href="/resource/?id={{RES_ID}}">Status</a>
    </td>
  </tr>
  {{/RES_DATA}}
</table>

<h2>Topology</h2>

<script src="http://d3js.org/d3.v3.min.js"></script>
<script>

var width = screen.availWidth,
    height = screen.availHeight;

var cluster = d3.layout.cluster()
    .size([width, height - 80]);

var diagonal = d3.svg.diagonal()
    .projection(function(d) { return [d.x, d.y]; });

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height)
  .append("g")
    .attr("transform", "translate(0,40)");

d3.json("/resources/topology", function(error, root) {
  var nodes = cluster.nodes(root),
      links = cluster.links(nodes);

  var link = svg.selectAll(".link")
      .data(links)
    .enter().append("path")
      .attr("class", "link")
      .attr("d", diagonal);

  var node = svg.selectAll(".node")
      .data(nodes)
    .enter().append("g")
      .attr("class", function(d) { return "node " + (d.resource_desc.schedulable == true ? "pu" : "other"); })
      .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; })
      .on("mouseover", mouseover)
      .on("mouseout", mouseout);

     node.append("circle")
         .attr("r", function(d) { return d.resource_desc.type == 0 ? 6.0 : 4.5; })
         .attr("fill", function(d) {
           switch (d.resource_desc.state) {
             case 0: return "gray";
             case 1: return "#00ff00";
             case 2: return "#ff0000";
             case 3: return "black";
             default: return "white";
           }
         });

 node.append("text")
     .attr("dx", function(d) { return d.children ? -8 : 8; })
     .attr("dy", 4)
     .style("text-anchor", function(d) { return d.children ? "end" : "start"; })
     .text(function(d) {
       return (d.resource_desc.friendly_name);
     });
});

function mouseover(d) {
  d3.select(this)
  .select("text")
  .text(function(d) { return d.resource_desc.uuid; })
}

function mouseout(d) {
  d3.select(this)
  .select("text")
  .text(function(d) { return d.resource_desc.friendly_name; })
}


d3.select(self.frameElement).style("height", height + "px");

</script>

{{>PAGE_FOOTER}}
