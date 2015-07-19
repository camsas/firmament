<html>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01//EN" "http://www.w3.org/TR/html4/strict.dtd">
<head>
  <title>Firmament</title>
  <style>
    body {
      margin: none;
      font-family: sans-serif;
      padding-top: 70px;
      padding-bottom: 30px;
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

    .theme-dropdown .dropdown-menu {
      position: static;
      display: block;
      margin-bottom: 20px;
    }

    .pre-x-scroll {
      overflow-x: scroll;
      word-wrap: normal;
      white-space: pre;
    }
  </style>

  <!-- JQuery -->
  <script src="http://code.jquery.com/jquery-1.9.1.min.js"></script>

  <!-- Bootstrap -->
  <!-- Latest compiled and minified CSS -->
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css">
  <!-- Optional theme -->
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap-theme.min.css">
  <!-- Latest compiled and minified JavaScript -->
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"></script>

  <!-- Sparklines for resource consumption -->
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery-sparklines/2.1.2/jquery.sparkline.min.js"></script>

  <!-- Friendly timestamps -->
  <script src="https://cdn.rawgit.com/rmm5t/jquery-timeago/master/jquery.timeago.js"></script>

  <!-- Flow graph visualisation -->
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.3.0/vis.min.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/vis/4.3.0/vis.min.css">

  <!-- d3 for visualisation -->
  <script src="http://d3js.org/d3.v3.min.js"></script>

  <!-- Workaround for JQS + bootstrap issue -->
  <style>
    .jqstooltip {
      width: auto !important;
      height: auto !important;
    }
  </style>
</head>

