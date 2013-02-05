<!-- 
   
   By Dimitar Popov (dpp23 at cam.ac.uk)

   Please report any bugs, which presumably exist.
   
-->

{{>HEADER}}
<head>
<script type="text/javascript" src="http://mbostock.github.com/d3/d3.js"></script>
<script type="text/javascript" src="http://mbostock.github.com/d3/d3.layout.js"></script>
<script type="text/javascript" src="http://mbostock.github.com/d3/d3.geom.js"></script>
<style type="text/css">
.link { stroke: #ccc; }
.nodetext { pointer-events: none; font: 10px sans-serif; }
</style>
</head>

{{>PAGE_HEADER}}

<div>
URL:<input type="text" name="user"><input type="button" value="Submit" class="butt"></form>
Base layout on:

<input type="checkbox" name="spawn" checked>Spawn relationships.  
<input type="checkbox" name="depend">Dependency relationships. 
<textarea name="info" class="info" cols="20" rows="5">
Point to an object to get more info.
</textarea>
</div>

<script src="http://ajax.googleapis.com/ajax/libs/jquery/1/jquery.min.js"></script>
<script type="text/javascript">

var pulled; //pulled data
var dep_strength = 0;
var spawn_strength = 0;
var dep_colour = "black";
var spawn_colour = "#ccc";
var url;
function pull(url)
{
  $.ajax({
         url: url,
         async: false,
         dataType: 'json',
         success: function(data) {
                 pulled=data;
         }});
}

			var nodes = [];
			var links = [];
			
			var w = 1100, h = 800;
			var c_width = 20
			var labelDistance = 0;
			
			//TODO: Legend
			function colour_from_state(k)
			{
			    switch(k)
			    {
				case 0: return "blue";
				case 1: return "green";
				case 2: return "red";
				case 3: return "brown";
				case 4: return "silver";
				case 5: return "orange";
				case 6: return "purple";
				default: return "yellow";
			    }
			}
			  
			//Find the index of an existing node in nodes by its id
			function find_index_by_id(id)
			{
			    for(var i=0; i < nodes.length; i++)
				  if(nodes[i].id == id) return i;
			}
			
			//Adds recursivelly all objects to nodes
			function add_node(dat, parent)
			{
			      dat.id = dat.uid;
			      var Member = 0;
			      for(var j=0; j < nodes.length; j++)
			      {
				    if(nodes[j].id == dat.id)
				    {
					      Member = 1;
					      nodes[j].state = dat.state;
					      break;
				    }
			      }
			      if ( Member == 0)
			      { 
				    dat.weight = 1000000; 
				    nodes.push(dat);
				    if(parent)
				    {
				      links.push(
						  {
						      source : parent,
						      target : dat,
						      weight : spawn_strength,
						      type : 0
						  });
				    }
				    else console.log("If this appears more than few times, something is wrong, pressumably force can't finish before the next step() call");
			      }
			      dat.shape = 0;
			      if(dat.dependencies.length >0)
			      {
				  for( var i=0; i < dat.dependencies.length; i++)
				  {
				      dat.dependencies[i].shape = 1;
				      dat.dependencies[i].id = - dat.dependencies[i].id;
				      var member = 0;
				      for(var j=0; j < nodes.length; j++)
				      {
					  if(nodes[j].id == dat.dependencies[i].id)
					  {
					      member = 1;
					      if( Member == 0 )
					      {
						  links.push(
							    {
							      source : nodes[j],
							      target : dat,
							      weight : dep_strength,
							      type : 1
							    });
						  break;
					      }						  
					  }
				      }
				      if(member == 0)
				      {
				      
					nodes.push(dat.dependencies[i]);
					if( Member == 0 )
					      {
						  links.push(
							{
							  source : dat.dependencies[i],
							  target : dat,
							  weight : dep_strength,
							  type : 1
							});
					      }
					else
					{
					    links.push(
							{
							  source : dat.dependencies[i],
							  target : nodes[find_index_by_id(dat.id)],
							  weight : dep_strength,
							  type : 1
							});
					}
				      }
				  }
			      }
			      if(dat.outputs.length >0)
			      {
				  for( var i=0; i < dat.outputs.length; i++)
				  {
				      dat.outputs[i].shape = 1;
				      dat.outputs[i].id = -dat.outputs[i].id;
				      var member = 0;
				      for(var j=0; j < nodes.length; j++)
				      {
					  if(nodes[j].id == dat.outputs[i].id) { member = 1; break;}
				      }
				      if( member == 0)
				      {
					nodes.push(dat.outputs[i]);
					if( Member == 0 )
					{
						links.push(
							{
							  source : dat,
							  target : dat.outputs[i],
							  weight : dep_strength,
							  type : 1
							});
					}
					else
					{
						links.push(
							{
							  source : nodes[find_index_by_id(dat.id)],
							  target : dat.outputs[i],
							  weight : dep_strength,
							  type : 1
							});
					}
				      }
				  }
			      }
			      if(dat.spawned.length > 0 )
			      {
				    var tmp = dat;
				    if(Member)tmp = nodes[find_index_by_id(dat.id)]
				    for(var i=0; i < dat.spawned.length; i++)add_node(dat.spawned[i], tmp);
			      }
			}
			

	var vis = d3.select("body").append("svg:svg")
		  .attr("class", "main")
		  .attr("width", w)
		  .attr("height", h);
		  
	var vis_link = vis.append("g");

	
	//playing with the charge, friction etc migth produce more stable results
	var force = self.force = d3.layout.force()
		      .nodes(nodes)
		      .links(links)
		      .gravity(0.5)
		      .distance(100)
		      .friction(0.3)
		      .theta(1)
		      //.alpha(1)
		      .charge(-10000)
		      .size([w, h]);
	
	force.on("tick", function() 
			  {
				var node = vis.selectAll("g.node")
					    .data(nodes )
				var link = vis.selectAll("line.link")
					  .data(links, function(d) { return d.source.id + ',' + d.target.id})
				link.attr("x1", function(d) { return d.source.x; })
					  .attr("y1", function(d) { return d.source.y; })
					  .attr("x2", function(d) { return d.target.x; })
					  .attr("y2", function(d) { return d.target.y; });
				node.attr("transform", function(d) 
							{  
								    //Adjust the graph to accommodate everything
								    w = Math.sqrt(nodes.length) * 200;
								    h = w;
								    d3.select(".main")
								      .attr("width", w)
								      .attr("height", h);
								    force.size([w,h]);
								  
								    return "translate(" + d.x + "," + d.y + ")";
							});
				//move rects in the middle
				d3.selectAll(".node_r").select(".node").attr("transform", function(d) { return "translate(" + (d.x - c_width) + "," + (d.y - c_width) + ")"; });
	});

//Adds new objects	
function recalc()
{
   
    var link = vis_link.selectAll("line.link")
                  .data(links);

    link.enter().append("svg:line")
                .attr("class", "link")
                .attr("x1", function(d) { return d.source.x + c_width; })
                .attr("y1", function(d) { return d.source.y + c_width; })
                .attr("x2", function(d) { return d.target.x + c_width; })
                .attr("y2", function(d) { return d.target.y + c_width; })
                .style("stroke", function(d) { if (d.type) return dep_colour; else return spawn_colour;});
                
    link.style("stroke", function(d) { if (d.type) return dep_colour; else return spawn_colour;});

    link.exit().remove();

    var node = vis.selectAll("g.node")
	.data(nodes);

    var nodeEnter = node.enter().append("g").attr("class", function(d) { if (d.shape == 1){ return "node_r";} else {return "node_c";} })
		      .append("svg:g")
		      .attr("class", "node");

   
    //The next two function calls ensure objects are not repeated one over another
    d3.selectAll(".node_c").select(".node").each(function()
						      {
							    if (d3.select(this).select("circle")[0][0] != null) return;
							    d3.select(this).append("svg:circle").attr("r", c_width).style("fill", "#256").style("stroke", "#FFF").style("stroke-width", 3);
							   });
    d3.selectAll(".node_r").select(".node").each(function()
						      {
							    if (d3.select(this).select("rect")[0][0] != null) return;
							    d3.select(this).append("svg:rect").attr("width", 2 * c_width).attr("height", 2 * c_width).style("fill", "#128").style("stroke", "#FFF").style("stroke-width", 3);
						      });
    //State to colour update
    node.each( function(d)
		{
		  d3.select(this).select("circle").style("fill", colour_from_state(d.state));
		  d3.select(this).select("rect").style("fill", colour_from_state(d.type));
		});
		
    node.exit().remove();
    
    //Add autohide ID labels
    nodeEnter.append("svg:text")
			.attr("class", "nodetext")
			.style("visible", 0)
			.attr("dx", 12)
			.attr("dy", ".35em")
			.style("visibility", "hidden")
			.text(function(d) { if (d.uid) return "ID: " +  d.uid; else  return "ID: " +  (-d.id); });
    
    //Make the ID labels autohide and load onMouseover the info in the textbox
    node.each(function(d)
	      {
	
		d3.selectAll(".node").on("mouseover", function(d)
			{
			      var id;
			      if (d.uid) id = "ID: " +  d.uid; else  id = "ID: " +  (-d.id) ;
			      var text = id + '\n' + "State: " + d.state + '\n' + "Name: " + d.name;
			      d3.select(this).select("text").style("visibility", "visible")
			      d3.select(".info").text(text);
							
			})
			 .on("mouseout", function()
			{
			      d3.select(this).select("text").style("visibility", "hidden")
			});
	      });
    force.start();
    

}

//Consisit of pull data and recalc, calls inself after 2 secs
function step()
{
      url = "/job/dtg/?id={{JOB_ID}}";
      pull(url);
      pulled.root_task.state = 1000;
      add_node(pulled.root_task, null);
      recalc();
      window.setTimeout(step, 2000);
}

//Initializating function
function start()
{
      d3.select(".info").style("visibility", "visible");
      if($('input[name=spawn]').is(':checked')) spawn_strength = 100;
      if($('input[name=depend]').is(':checked')) dep_strength = 100;
      step();
}

d3.select(".info").style("visibility", "hidden")
//d3.select(".butt").attr("onClick", "start()");
window.onload = function() {
  start();
}

</script>

{{>PAGE_FOOTER}}
