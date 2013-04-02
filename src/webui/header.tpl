<html>
<head>
  <title>Firmament</title>
  <style>
    body {
      margin: none;
      font-family: sans-serif;
    }
    table {
      border: 1px solid black;
      border-collapse: collapse;
    }
    th, td {
      border: 1px solid gray;
      padding: 2px;
    }

    /* classes for resource topology using d3 */
    .node circle {
      stroke: black;
      stroke-width: 1.5px;
    }

    .pu circle {
      stroke: steelblue;
      stroke-width: 2px;
    }

    .other circle {
      fill: #fff;
      stroke: gray;
    }

    .node circle .state_idle {
      fill: green;
    }

    .node {
      font: 10pt sans-serif;
    }

    .link {
      fill: none;
      stroke: #ccc;
      stroke-width: 2px;
    }
  </style>
  <script src="http://code.jquery.com/jquery-1.9.1.min.js"></script>
  <script src="http://www.omnipotent.net/jquery.sparkline/2.1.1/jquery.sparkline.min.js"></script>
</head>

