window._tb_chart_payloads = window._tb_chart_payloads || {};

// Charting functions
function candleTrace(obj, name) {
  return {
    x: obj.labels,
    open: obj.open,
    high: obj.high,
    low: obj.low,
    close: obj.close,
    type: "candlestick",
    name: name,
    increasing: { line: { color: "#0b9444" } },
    decreasing: { line: { color: "#ef4444" } },
  };
}
function lineTrace(x, y, name, dash) {
  return {
    x: x,
    y: y,
    mode: "lines",
    name: name,
    line: { dash: dash || "solid", width: 1 },
  };
}
function markersScatter(x, y, color, name, size) {
  return {
    x: x,
    y: y,
    mode: "markers+text",
    name: name,
    marker: { color: color, size: size, symbol: "circle" },
    text: y.map((v) => (v ? "$" + parseFloat(v).toFixed(2) : "")),
    textposition: "top center",
  };
}

function renderChartForPayload(div_id, obj) {
  var node = document.getElementById(div_id);
  if (!node) return;
  try {
    node.setAttribute("data-rendered", "0");
    if (!obj || !obj.data || !obj.data.labels || obj.data.labels.length === 0) {
      node.innerHTML = '<div style="padding:12px;color:#666">No data</div>';
      node.setAttribute("data-rendered", "1");
      return;
    }
    // build traces
    var traces = [candleTrace(obj.data, "Candles")];
    if (obj.data.bb_upper && obj.data.bb_upper.length === obj.data.labels.length) {
      traces.push(lineTrace(obj.data.labels, obj.data.bb_upper, "BB Upper", "dot"));
      traces.push(lineTrace(obj.data.labels, obj.data.bb_mid, "BB Mid", "dash"));
      traces.push(lineTrace(obj.data.labels, obj.data.bb_lower, "BB Lower", "dot"));
    }
    // markers
    if (obj.markers && obj.markers.length > 0) {
      var xp = [],
        yp = [],
        xt = [],
        yt = [];
      obj.markers.forEach(function (m) {
        if (
          typeof m.pos === "number" &&
          m.pos >= 0 &&
          m.pos < obj.data.labels.length
        ) {
          var xlbl = obj.data.labels[m.pos];
          var price = m.price;
          if (m.type === "peak") {
            xp.push(xlbl);
            yp.push(price);
          } else if (m.type === "trough") {
            xt.push(xlbl);
            yt.push(price);
          }
        }
      });
      if (xp.length) {
        traces.push(markersScatter(xp, yp, "#ef4444", "Tops", 12));
      }
      if (xt.length) {
        traces.push(markersScatter(xt, yt, "#10b981", "Bottoms", 12));
      }
    }
    
    var layout = {
      margin: { t: 8, b: 28, l: 40, r: 16 },
      xaxis: { rangeslider: { visible: false } },
      legend: { orientation: "h" },
      height: %HEIGHT%, // Templated height
    };
    Plotly.newPlot(div_id, traces, layout, {
      displayModeBar: false,
      responsive: true,
    });
    node.setAttribute("data-rendered", "1");
  } catch (err) {
    try {
      node.innerHTML =
        '<div style="padding:12px;color:#900">Chart render failed: ' +
        (err && err.message ? err.message : String(err)) +
        "</div>";
      node.setAttribute("data-rendered", "1");
    } catch (e) {}
    console.log("renderChartForPayload error", err);
  }
}

// Lazy-load charts on scroll
function autoRenderAll() {
  var payloads = window._tb_chart_payloads || {};
  var ids = Object.keys(payloads);
  // Eager load first N
  for (var i = 0; i < ids.length && i < %EAGER%; i++) {
    try {
      renderChartForPayload(ids[i], payloads[ids[i]]);
    } catch (e) {}
  }
  // Lazy load the rest
  if ("IntersectionObserver" in window) {
    var obs = new IntersectionObserver(
      function (entries, o) {
        entries.forEach(function (ent) {
          if (ent.isIntersecting) {
            var id = ent.target.id;
            if (id && window._tb_chart_payloads && window._tb_chart_payloads[id])
              try {
                renderChartForPayload(id, window._tb_chart_payloads[id]);
              } catch (e) {}
            o.unobserve(ent.target);
          }
        });
      },
      { root: null, rootMargin: "200px", threshold: 0.05 }
    );
    ids.forEach(function (id) {
      var n = document.getElementById(id);
      if (n && n.getAttribute("data-rendered") !== "1") obs.observe(n);
    });
  } else {
    // Fallback for older browsers
    ids.forEach(function (id) {
      var n = document.getElementById(id);
      if (n && n.getAttribute("data-rendered") !== "1")
        try {
          renderChartForPayload(id, window._tb_chart_payloads[id]);
        } catch (e) {}
    });
  }
}

// --- Filters: multi-select AND logic (FIXED) ---
let activeFilters = [];
function toggleTagButton(btn, tag) {
  try {
    var el =
      btn instanceof HTMLElement
        ? btn
        : document.querySelector('[data-filter="' + tag + '"]');
    if (!el) el = btn;
    var idx = activeFilters.indexOf(tag);
    if (idx === -1) {
      activeFilters.push(tag);
      el.classList.add("active");
    } else {
      activeFilters.splice(idx, 1);
      el.classList.remove("active");
    }
    applyCombinedFilter();
    updateStatus();
  } catch (e) {
    console.log("toggleTagButton error", e);
  }
}

function applyCombinedFilter() {
  var allCards = Array.from(document.querySelectorAll(".signal_card"));
  var noResultsMsg = document.getElementById("no_results_msg");
  var visibleCount = 0;

  if (activeFilters.length === 0) {
    allCards.forEach(function (c) {
      c.style.display = "block";
      visibleCount++;
    });
  } else {
    allCards.forEach(function (c) {
      var tags = (c.dataset.tags || "")
        .split(",")
        .map((x) => x.trim())
        .filter((x) => x);
      
      // AND logic: .every()
      var ok = activeFilters.every(function (f) {
        return tags.indexOf(f) !== -1;
      });

      c.style.display = ok ? "block" : "none";
      if (ok) visibleCount++;
    });
  }

  // Show/hide "No results" message
  if (noResultsMsg) {
    noResultsMsg.style.display = visibleCount === 0 ? "block" : "none";
  }
}

function updateStatus() {
  var st = document.getElementById("statusMsg");
  if (!st) return;
  st.textContent =
    "Mode: " +
    (document.getElementById("modeSelect")
      ? document.getElementById("modeSelect").value
      : "STRICT") +
    (activeFilters.length
      ? " • Filters: " + activeFilters.join(", ")
      : " • Filters: none");
}

function filterGroup(mode) {
  var noResultsMsg = document.getElementById("no_results_msg");
  if (noResultsMsg) noResultsMsg.style.display = "none"; // Hide on reset

  if (mode === "ALL") {
    activeFilters = [];
    document
      .querySelectorAll("[data-filter]")
      .forEach((b) => b.classList.remove("active"));
    applyCombinedFilter();
    updateStatus();
    return;
  }
  // This function is for single-tag toggling, which is what the buttons do
  toggleTagButton(
    document.querySelector('[data-filter="' + mode + '"]'),
    mode
  );
}

// --- Toggle Table (FIXED) ---
function toggleTable(div_id) {
  var tableDiv = document.getElementById(div_id + "_table");
  if (!tableDiv) return;
  if (tableDiv.style.display === "block") {
    tableDiv.style.display = "none";
    return;
  }
  
  // Read from 'tableData' key instead of 'last30'
  var payload = window._tb_chart_payloads && window._tb_chart_payloads[div_id];
  if (
    !payload ||
    !payload.tableData ||
    !payload.tableData.labels ||
    payload.tableData.labels.length === 0
  ) {
    tableDiv.innerHTML =
      '<div style="padding:12px;color:#666">No table data available</div>';
    tableDiv.style.display = "block";
    return;
  }

  var html =
    '<table style="width:100%;border-collapse:collapse;"><thead><tr><th>Date</th><th>Open</th><th>High</th><th>Low</th><th>Close</th><th>Vol</th></tr></thead><tbody>';
  
  var L = payload.tableData.labels.length;
  // We read all rows from tableData (it's already pre-sliced in Python)
  for (var i = 0; i < L; i++) {
    var d = payload.tableData.labels[i];
    var o =
      payload.tableData.open && payload.tableData.open[i] !== null
        ? payload.tableData.open[i]
        : "";
    var h =
      payload.tableData.high && payload.tableData.high[i] !== null
        ? payload.tableData.high[i]
        : "";
    var l =
      payload.tableData.low && payload.tableData.low[i] !== null
        ? payload.tableData.low[i]
        : "";
    var c =
      payload.tableData.close && payload.tableData.close[i] !== null
        ? payload.tableData.close[i]
        : "";
    var v =
      payload.tableData.volume && payload.tableData.volume[i] !== null
        ? payload.tableData.volume[i]
        : "";

    html +=
      '<tr style="border-top:1px solid #eef2ff"><td style="padding:6px">' +
      d +
      '</td><td style="padding:6px">' +
      (o ? "$" + parseFloat(o).toFixed(2) : "n/a") +
      '</td><td style="padding:6px">' +
      (h ? "$" + parseFloat(h).toFixed(2) : "n/a") +
      '</td><td style="padding:6px">' +
      (l ? "$" + parseFloat(l).toFixed(2) : "n/a") +
      '</td><td style="padding:6px">' +
      (c ? "$" + parseFloat(c).toFixed(2) : "n/a") +
      '</td><td style="padding:6px">' +
      v +
      "</td></tr>";
  }
  html += "</tbody></table>";
  tableDiv.innerHTML = html;
  tableDiv.style.display = "block";
}

// --- Other Controls ---

// CSV download
function downloadCSV() {
  var rows = [];
  document.querySelectorAll(".signal_card").forEach(function (c) {
    if (c.style.display === "none") return; // Only download visible
    rows.push({
      ticker: c.dataset.ticker || "",
      tags: c.dataset.tags || "",
    });
  });
  if (rows.length === 0) {
    alert("No visible rows to download. Clear filters to download all.");
    return;
  }
  var csv = "ticker,tags\n";
  rows.forEach(function (r) {
    csv += r.ticker + "," + '"' + r.tags + '"' + "\n";
  });
  var blob = new Blob([csv], { type: "text/csv" });
  var url = URL.createObjectURL(blob);
  var a = document.createElement("a");
  a.href = url;
  a.download = "TopBottom_filtered.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
}

// manual refresh
function manualRefresh() {
  window.location.reload();
}

// auto-refresh
var _autoRefreshHandle = null;
function toggleAutoRefresh(enabled) {
  if (enabled) {
    var mins = %REF%;
    if (_autoRefreshHandle) clearInterval(_autoRefreshHandle);
    _autoRefreshHandle = setInterval(function () {
      window.location.reload();
    }, mins * 60 * 1000);
    alert("Auto-refresh enabled: every " + mins + " minutes");
  } else {
    if (_autoRefreshHandle) clearInterval(_autoRefreshHandle);
    _autoRefreshHandle = null;
    alert("Auto-refresh disabled");
  }
}

// mode set
function setMode(v) {
  // Mode selection is just for show, not tied to logic currently
  updateStatus();
}

// On page load
document.addEventListener("DOMContentLoaded", function () {
  try {
    document.getElementById("modeSelect").value = "STRICT";
    setMode("STRICT");
  } catch (e) {}
  setTimeout(autoRenderAll, 450); // Start rendering charts
});