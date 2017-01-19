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

    .long-hostname {
      padding-top: 50px;
    }
  </style>

  <!-- JQuery -->
  <script src="http://code.jquery.com/jquery-1.9.1.min.js"></script>

  <!-- Bootstrap -->
  <!-- Latest compiled and minified CSS -->
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css" />
  <!-- Optional theme -->
  <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap-theme.min.css" />
  <!-- Latest compiled and minified JavaScript -->
  <script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"></script>

  <!-- Sparklines for resource consumption -->
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery-sparklines/2.1.2/jquery.sparkline.min.js"></script>

  <!-- Friendly timestamps -->
  <script src="https://cdn.rawgit.com/rmm5t/jquery-timeago/master/jquery.timeago.js"></script>

  <!-- Flow graph visualisation -->
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.3.0/vis.min.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/vis/4.3.0/vis.min.css" />

  <!-- d3 for visualisation -->
  <script src="http://d3js.org/d3.v3.min.js"></script>

  <!-- Table sorter plugin for JQuery -->
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.22.3/js/jquery.tablesorter.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.22.3/js/jquery.tablesorter.widgets.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.22.3/css/theme.bootstrap.min.css" />

  <!-- Workaround for JQS + bootstrap issue -->
  <style>
    .jqstooltip {
      width: auto !important;
      height: auto !important;
    }
  </style>

  <script>
  $.tablesorter.themes.bootstrap = {
    // these classes are added to the table
    table        : 'table table-bordered table-striped',
    caption      : 'caption',
    // header class names
    header       : 'bootstrap-header', // give the header a gradient background (theme.bootstrap_2.css)
    sortNone     : '',
    sortAsc      : '',
    sortDesc     : '',
    active       : '', // applied when column is sorted
    hover        : '', // custom css required - a defined bootstrap style may not override other classes
    // icon class names
    icons        : '', // add "icon-white" to make them white; this icon class is added to the <i> in the header
    iconSortNone : 'bootstrap-icon-unsorted', // class name added to icon when column is not sorted
    iconSortAsc  : 'glyphicon glyphicon-chevron-up', // class name added to icon when column has ascending sort
    iconSortDesc : 'glyphicon glyphicon-chevron-down', // class name added to icon when column has descending sort
    filterRow    : '', // filter row class; use widgetOptions.filter_cssFilter for the input/select element
    footerRow    : '',
    footerCells  : '',
    even         : '', // even row zebra striping
    odd          : ''  // odd row zebra striping
  };
  </script>
</head>
