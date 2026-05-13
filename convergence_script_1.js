
// region Helpers and General
function extractHHMM(timeValue) {
    if (timeValue === null || timeValue === undefined) return null;

    let s = String(timeValue).trim();
    if (!s) return null;

    if (s.includes("T")) s = s.split("T").pop();
    if (s.includes(" ")) s = s.split(" ").pop();
    if (s.includes("+")) s = s.split("+")[0];
    if (s.includes(".")) s = s.split(".")[0];

    const match = s.match(/(\d{1,2}):(\d{2})(?::(\d{2}))?/);
    if (!match) return null;

    const hh = parseInt(match[1], 10);
    const mm = parseInt(match[2], 10);
    if (Number.isNaN(hh) || Number.isNaN(mm)) return null;

    const hh2 = ((hh % 24) + 24) % 24;
    return String(hh2).padStart(2, "0") + ":" + String(mm).padStart(2, "0");
}

function timeToY(timeValue) {
    const hhmm = extractHHMM(timeValue);
    if (!hhmm) return null;

    const parts = hhmm.split(":").map(Number);
    if (parts.length < 2 || Number.isNaN(parts[0]) || Number.isNaN(parts[1])) return null;

    const h = parts[0];
    const m = parts[1];

    let minutes = h * 60 + m;
    const anchor = 5 * 60;

    if (minutes < anchor) {
        minutes += 24 * 60;
    }

    const relative = minutes - anchor;

    const topY = 40;
    const bottomY = 1920;
    const fraction = relative / (24 * 60);

    return topY + fraction * (bottomY - topY);}

function toPercentNumber(v) {
    if (v === null || v === undefined) return null;
    const raw = String(v).trim();
    if (!raw) return null;

    // Support formats like "85", "85.2", "85%", "85,2%".
    const normalized = raw.replace("%", "").replace(",", ".");
    const n = Number(normalized);
    return Number.isFinite(n) ? Math.round(n) : null;
}

function updateTitleFromState() {
    const titleEl = document.getElementById("stationTitle");
    if (!titleEl) return;

    const hasYM = Number.isFinite(activeYear) && Number.isFinite(activeMonth);
    const ymTxt = hasYM ? (" | " + activeYear + "-" + String(activeMonth).padStart(2, "0")) : "";

    titleEl.innerText = station
        ? ("סנכרון: " + station + ymTxt)
        : ("סנכרון" + ymTxt);
}

function closeManualSyncPopups() {
    const popup = document.querySelector(".manual-sync-popup");
    const historyPopup = document.querySelector(".manual-sync-history");
    const simulationPopup = document.querySelector(".manual-sync-simulation");
    const confirmPopup = document.querySelector(".manual-sync-confirm");
    if (popup) popup.classList.remove("open");
    if (historyPopup) historyPopup.classList.remove("open");
    if (simulationPopup) simulationPopup.classList.remove("open");
    if (confirmPopup) confirmPopup.classList.remove("open");
}
// endregion Helpers and General


// region prepering the data for bus_to_rail and rail_to_bus
const BUS_TO_RAIL_JSON = JSON.parse(
    document.getElementById("bus-to-rail-data").textContent
  );

const RAIL_TO_BUS_JSON = JSON.parse(
    document.getElementById("rail-to-bus-data").textContent
  );

const BUS_TO_RAIL_TREND_JSON = JSON.parse(
    document.getElementById("bus-to-rail-trend-data").textContent
  );


let stationTimes = [];

function rebuildStationTimes() {
    stationTimes = (activeDirection === BUS_TO_RAIL_DIRECTION)
      ? BUS_TO_RAIL_JSON
      : RAIL_TO_BUS_JSON;
}

function getWeekFiltered(rows = stationTimes) {
    return rows.filter(r => String(r[KEY_WEEK_PERIOD] ?? "").trim() === activeWeek);
}

function getOperatorFiltered(rows) {
    if (!activeOperator) return rows;
    return rows.filter(r => String(r[KEY_OPERATOR] ?? "").trim() === activeOperator);
}

function getFilteredTimes() {
    return getOperatorFiltered(getWeekFiltered(stationTimes));
}


// endregion prepering the data for


// region year/month dropdown filter:
const YEAR_MONTH_PAIRS = JSON.parse(
    document.getElementById("year-month-pairs-data").textContent
  );

const KEY_YEAR = "שנה";
const KEY_MONTH = "חודש";

const params = new URLSearchParams(window.location.search);
const station = (params.get("station") || "").trim();
const yearStr = params.get("year");
const monthStr = params.get("month");

function setBackLinkWithCurrentFilters() {
    const link = document.getElementById("backToMainPageLink");
    if (!link) return;
    const baseUrl = link.getAttribute("href") || "";
    const qp = new URLSearchParams();
    if (station) qp.set("station", station);
    if (yearStr) qp.set("year", yearStr);
    if (monthStr) qp.set("month", monthStr);
    link.href = qp.toString() ? `${baseUrl}?${qp.toString()}` : baseUrl;
}

const selectedYear = yearStr ? parseInt(yearStr, 10) : null;
let activeYear = selectedYear

const selectedMonth = monthStr ? parseInt(monthStr, 10) : null;
let activeMonth = selectedMonth

const yearSelect = document.createElement("select");
yearSelect.className = "filter-select";

const monthSelect = document.createElement("select");
monthSelect.className = "filter-select";


function getYearMonthPairsForStation() {
    return (YEAR_MONTH_PAIRS || [])
        .map(p => ({ y: parseInt(p.year, 10), m: parseInt(p.month, 10) }))
        .filter(p => Number.isFinite(p.y) && Number.isFinite(p.m));
}

function populateYearDropdown() {
    const pairs = getYearMonthPairsForStation();
    const years = Array.from(new Set(pairs.map(p => p.y))).sort((a, b) => a - b);

    yearSelect.innerHTML = "";
    years.forEach(y => {
        const opt = document.createElement("option");
        opt.value = String(y);
        opt.textContent = String(y);
        yearSelect.appendChild(opt);
    });
    yearSelect.value = String(activeYear);
}

function populateMonthDropdown() {
    const ySel = parseInt(yearSelect.value, 10);
    const pairs = getYearMonthPairsForStation();

    const months = Array.from(new Set(
        pairs.filter(p => p.y === ySel).map(p => p.m)
    )).sort((a, b) => a - b);

    monthSelect.innerHTML = "";
    months.forEach(m => {
        const opt = document.createElement("option");
        opt.value = String(m);
        opt.textContent = String(m).padStart(2, "0");
        monthSelect.appendChild(opt);
    });
    monthSelect.value = String(activeMonth);
}

function navigateWithYearMonth() {
    if (!station || !Number.isFinite(activeYear) || !Number.isFinite(activeMonth)) return;

    const p = new URLSearchParams(window.location.search);
    p.set("station", station);
    p.set("year", String(activeYear));
    p.set("month", String(activeMonth));

    window.location.href = "/convergence/?" + p.toString();
}

yearSelect.addEventListener("change", () => {
    const ySel = parseInt(yearSelect.value, 10);
    activeYear = Number.isFinite(ySel) ? ySel : null;

    populateMonthDropdown();

    const mSel = parseInt(monthSelect.value, 10);
    activeMonth = Number.isFinite(mSel) ? mSel : null;

    navigateWithYearMonth();
});
monthSelect.addEventListener("change", () => {
    const mSel = parseInt(monthSelect.value, 10);
    activeMonth = Number.isFinite(mSel) ? mSel : null;

    navigateWithYearMonth();
});
// endregion year/month dropdown filter


// region train direction dropdown filter
const KEY_RAIL_DIRECTION = "כיוון נסיעת הרכבת";

const BUS_TO_RAIL_DIRECTION = String(BUS_TO_RAIL_JSON[0][KEY_RAIL_DIRECTION]).trim();
const RAIL_TO_BUS_DIRECTION = String(RAIL_TO_BUS_JSON[0][KEY_RAIL_DIRECTION]).trim();

let activeDirection = BUS_TO_RAIL_DIRECTION; // default

const dirSelect = document.createElement("select");
dirSelect.className = "filter-select";

function populateDirectionDropdown() {
    const dirs = [BUS_TO_RAIL_DIRECTION, RAIL_TO_BUS_DIRECTION];
    dirSelect.innerHTML = "";

    dirs.forEach(d => {
      const opt = document.createElement("option");
      opt.value = d;
      opt.textContent = d;
      dirSelect.appendChild(opt);
    });

    dirSelect.value = activeDirection;
}

populateDirectionDropdown();

function refreshAllAfterFilterChange() {
    exitLinkMode();
    activeBusRowKey = null;
    activeSignage = null;

    rebuildStationTimes();
    populateWeekDropdown();
    populateOperatorDropdown();
    rebuildFilteredRawBusData();

    setActiveTrain(null);
    redrawTrains();
    redrawBuses();
    renderBusTable();
}

dirSelect.addEventListener("change", () => {
    activeDirection = dirSelect.value;
    refreshAllAfterFilterChange();
});

// endregion train direction dropdown filter


// region WeekPeriod filter
const KEY_WEEK_PERIOD = "תקופת שבוע";

const weekSelect = document.createElement("select");
weekSelect.className = "filter-select";

let activeWeek = "יום חול"; //defualt mode

function populateWeekDropdown() {
    weekSelect.innerHTML = "";

    const set = new Set();
    stationTimes.forEach(r => {
        const v = String(r[KEY_WEEK_PERIOD] ?? "").trim();
        if (v) set.add(v);
    });

    const weeks = Array.from(set).sort();

    const weekOrder = ["יום חול", "שישי", "שבת"];
    weeks.sort((a, b) => {
        const ai = weekOrder.indexOf(a);
        const bi = weekOrder.indexOf(b);
        const ar = ai === -1 ? Number.MAX_SAFE_INTEGER : ai;
        const br = bi === -1 ? Number.MAX_SAFE_INTEGER : bi;
        return ar - br || a.localeCompare(b, "he");
    });

    weeks.forEach(v => {
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        weekSelect.appendChild(opt);
    });

    weekSelect.value = activeWeek;
}

weekSelect.addEventListener("change", () => {
    exitLinkMode();
    activeWeek = weekSelect.value;
    rebuildFilteredRawBusData();
    populateOperatorDropdown();

    activeBusRowKey = null;
    activeSignage = null;
    setActiveTrain(null);

    redrawTrains();
    redrawBuses();
    renderBusTable();
});

// endregion WeekPeriod filter


// region Operator filter
const operatorSelect = document.createElement("select");
operatorSelect.className = "filter-select";

const KEY_OPERATOR = "מפעיל";
let activeOperator = "";


function populateOperatorDropdown() {
    operatorSelect.innerHTML = "";
    const allOpt = document.createElement("option");
    allOpt.value = "";
    allOpt.textContent = KEY_OPERATOR;
    operatorSelect.appendChild(allOpt);
    const set = new Set();
    stationTimes
        .filter(r => String(r[KEY_WEEK_PERIOD] ?? "").trim() === activeWeek)
        .forEach(r => {
            const v = String(r[KEY_OPERATOR] ?? "").trim();
            if (v) set.add(v);
        });
    const operators = Array.from(set).sort((a, b) => a.localeCompare(b, "he"));
    operators.forEach(v => {
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        operatorSelect.appendChild(opt);
    });
    if (activeOperator && operators.includes(activeOperator)) {
        operatorSelect.value = activeOperator;
    } else {
        activeOperator = "";
        operatorSelect.value = "";
    }
}

operatorSelect.addEventListener("change", () => {
    exitLinkMode();
    activeOperator = operatorSelect.value;

    activeBusRowKey = null;
    activeSignage = null;
    setActiveTrain(null);

    redrawTrains();
    redrawBuses();
    renderBusTable();
});
// endregion Operator filter


// region reset button
//for ESC button:
function clearMapAndTableFiltersOnly() {
    exitLinkMode();
    activeSignage = null;
    activeBusRowKey = null;
    setActiveTrain(null);

    closeManualSyncPopups();
    updateBusHeaderSelectionVisuals();
    applyBusFilter();
    renderBusTable();
    resetSimulationCapture();
}

const resetBtn = document.createElement("button");
resetBtn.textContent = "איפוס המפה";
resetBtn.className = "filter-select";


resetBtn.addEventListener("click", () => {
  clearMapAndTableFiltersOnly();
});

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
      clearMapAndTableFiltersOnly();
  }
});
const openStationTrendBtn = document.getElementById("openStationTrendBtn");
const h2 = document.querySelector("h2");
if (h2 && h2.parentNode) {
    const insertAfterNode = openStationTrendBtn || h2;
    h2.parentNode.insertBefore(resetBtn, insertAfterNode.nextSibling);
}

resetBtn.parentNode.insertBefore(dirSelect, resetBtn.nextSibling);
resetBtn.parentNode.insertBefore(yearSelect, dirSelect.nextSibling);
resetBtn.parentNode.insertBefore(monthSelect, yearSelect.nextSibling);
resetBtn.parentNode.insertBefore(weekSelect, monthSelect.nextSibling);
resetBtn.parentNode.insertBefore(operatorSelect, weekSelect.nextSibling);







// endregion reset button


// region percentage graph
const KEY_ON_TIME_PERCENT_BY_TRAIN = "אחוז הנסיעות שעמדו בזמנים ברמת נסיעת הרכבת";
const trendTitle = document.getElementById("trainTrendTitle");
const trendModal = document.getElementById("trainTrendModal");
const trendHost = document.getElementById("trainTrendChartHost");
const closeTrendBtn = document.getElementById("closeTrainTrendBtn");

function buildTrendKey(trainId, row) {
      const dir = String(row[KEY_RAIL_DIRECTION] ?? "").trim().replace(/\s+/g, " ");
      const normalizedTime = extractHHMM(row[KEY_TRAIN_ARRIVAL_RISHUI] ?? "");
      if (!trainId || !normalizedTime || !dir) return null;
      return String(trainId).trim() + "||" + normalizedTime + "||" + dir;
  }


function closeTrainTrendModal() {
    if (!trendModal) return;
    trendModal.classList.remove("open");
}

function renderTrainTrendChart(series) {
    if (!trendHost) return;
    trendHost.innerHTML = "";

    if (!series || !series.length) {
        const empty = document.createElement("div");
        empty.textContent = "אין נתונים עבור רכבת זו.";
        trendHost.appendChild(empty);
        return;
    }

    const points = [...series].sort((a, b) => {
        const ay = parseInt(a.year, 10);
        const am = parseInt(a.month, 10);
        const by = parseInt(b.year, 10);
        const bm = parseInt(b.month, 10);
        if (ay !== by) return ay - by;
        return am - bm;
    });

    const ns = "http://www.w3.org/2000/svg";
    const width = 780;
    const height = 320;
    const margin = { top: 20, right: 20, bottom: 58, left: 52 };
    const plotW = width - margin.left - margin.right;
    const plotH = height - margin.top - margin.bottom;

    const svgEl = document.createElementNS(ns, "svg");
    svgEl.setAttribute("width", "100%");
    svgEl.setAttribute("viewBox", "0 0 " + width + " " + height);

    const yMin = 0;
    const yMax = 100;
    const xStep = points.length > 1 ? (plotW / (points.length - 1)) : 0;

    function toY(v) {
        const n = Number(v);
        const clamped = Number.isFinite(n) ? Math.max(yMin, Math.min(yMax, n)) : yMin;
        const t = (clamped - yMin) / (yMax - yMin);
        return margin.top + (1 - t) * plotH;
    }

    function toX(i) {
        return margin.left + (i * xStep);
    }

    const axisX = document.createElementNS(ns, "line");
    axisX.setAttribute("x1", String(margin.left));
    axisX.setAttribute("y1", String(margin.top + plotH));
    axisX.setAttribute("x2", String(margin.left + plotW));
    axisX.setAttribute("y2", String(margin.top + plotH));
    axisX.setAttribute("stroke", "#444");
    svgEl.appendChild(axisX);

    const axisY = document.createElementNS(ns, "line");
    axisY.setAttribute("x1", String(margin.left));
    axisY.setAttribute("y1", String(margin.top));
    axisY.setAttribute("x2", String(margin.left));
    axisY.setAttribute("y2", String(margin.top + plotH));
    axisY.setAttribute("stroke", "#444");
    svgEl.appendChild(axisY);

    [0, 25, 50, 75, 100].forEach(tick => {
        const y = toY(tick);
        const grid = document.createElementNS(ns, "line");
        grid.setAttribute("x1", String(margin.left));
        grid.setAttribute("y1", String(y));
        grid.setAttribute("x2", String(margin.left + plotW));
        grid.setAttribute("y2", String(y));
        grid.setAttribute("stroke", "#e8e8e8");
        svgEl.appendChild(grid);

        const label = document.createElementNS(ns, "text");
        label.textContent = tick + "%";
        label.setAttribute("x", String(margin.left - 35));
        label.setAttribute("y", String(y + 4));
        label.setAttribute("text-anchor", "end");
        label.setAttribute("font-size", "11");
        label.setAttribute("fill", "#444");
        svgEl.appendChild(label);
    });

    let pathData = "";
    points.forEach((p, i) => {
        const x = toX(i);
        const y = toY(p.perc);
        pathData += (i === 0 ? "M" : " L") + x + " " + y;
    });

    const line = document.createElementNS(ns, "path");
    line.setAttribute("d", pathData);
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", "#0b5fff");
    line.setAttribute("stroke-width", "2");
    svgEl.appendChild(line);

    points.forEach((p, i) => {
        const x = toX(i);
        const y = toY(p.perc);
        const monthNum = parseInt(p.month, 10);
        const ymLabel = String(p.year) + "-" + String(monthNum).padStart(2, "0");

        const dot = document.createElementNS(ns, "circle");
        dot.setAttribute("cx", String(x));
        dot.setAttribute("cy", String(y));
        dot.setAttribute("r", "3.5");
        dot.setAttribute("fill", "#0b5fff");
        const title = document.createElementNS(ns, "title");
        title.textContent = String(p.perc) + "%";
        dot.appendChild(title);
        svgEl.appendChild(dot);

        const xLabel = document.createElementNS(ns, "text");
        xLabel.textContent = ymLabel;
        xLabel.setAttribute("x", String(x));
        xLabel.setAttribute("y", String(margin.top + plotH + 20));
        xLabel.setAttribute("text-anchor", "middle");
        xLabel.setAttribute("font-size", "11");
        xLabel.setAttribute("fill", "#333");
        svgEl.appendChild(xLabel);
    });

    trendHost.appendChild(svgEl);
}

function openTrainTrendModal(trainId, row) {
  if (!trendModal || !trendHost || !trendTitle) return;

  const trendKey = buildTrendKey(trainId, row);
  if (!trendKey) {
      renderTrainTrendChart([]);
      trendModal.classList.add("open");
      return;
  }

  // Keep trend within the selected week period
  const selectedWeek = String(activeWeek ?? "").trim();

  const byYm = new Map();
  for (const r of BUS_TO_RAIL_TREND_JSON) {
      const rid = getTrainIdValue(r);
      const rk = buildTrendKey(rid, r);
      if (!rk || rk !== trendKey) continue;

      const week = String(r[KEY_WEEK_PERIOD] ?? "").trim();
      if (selectedWeek && week !== selectedWeek) continue;

      const y = parseInt(r[KEY_YEAR], 10);
      const m = parseInt(r[KEY_MONTH], 10);
      const p = toPercentNumber(r[KEY_ON_TIME_PERCENT_BY_TRAIN]);
      if (!Number.isFinite(y) || !Number.isFinite(m) || p === null) continue;

      const ymKey = `${y}-${String(m).padStart(2, "0")}`;
      if (!byYm.has(ymKey)) {
          byYm.set(ymKey, { year: y, month: m, perc: p });
      }
  }

  const series = Array.from(byYm.values()).sort(
      (a, b) => (a.year - b.year) || (a.month - b.month)
  );

  trendTitle.textContent =
      "אחוזי סנכרון עבור רכבת " + trainId + (selectedWeek ? " | " + selectedWeek : "");

  renderTrainTrendChart(series);
  trendModal.classList.add("open");
}

if (closeTrendBtn) {
    closeTrendBtn.addEventListener("click", closeTrainTrendModal);
}

if (trendModal) {
    trendModal.addEventListener("click", (e) => {
        if (e.target === trendModal) closeTrainTrendModal();
    });
}

// endregion percentage graph


// region percentage graph STATION LEVEL
const COL_PERC_BY_TRAIN_STATION = "אחוז הנסיעות שעמדו בזמנים ברמת תחנת רכבת";

function renderStationTrendChart(rows) {
    if (!trendHost) return;
    trendHost.innerHTML = "";

    if (!Array.isArray(rows) || !rows.length) {
        const empty = document.createElement("div");
        empty.textContent = "אין נתונים ברמת תחנה.";
        trendHost.appendChild(empty);
        return;
    }

    const byYm = new Map();
    for (const r of rows) {
        const y = parseInt(r[KEY_YEAR], 10);
        const m = parseInt(r[KEY_MONTH], 10);
        if (!Number.isFinite(y) || !Number.isFinite(m)) continue;
        const p = toPercentNumber(r[COL_PERC_BY_TRAIN_STATION]);
        if (p === null) continue;
        const ymKey = `${y}-${String(m).padStart(2, "0")}`;
        if (!byYm.has(ymKey)) {
            byYm.set(ymKey, { year: y, month: m, perc: Math.max(0, Math.min(100, p)) });
        }
    }

    const points = Array.from(byYm.values()).sort(
        (a, b) => (a.year - b.year) || (a.month - b.month)
    );

    if (!points.length) {
        const empty = document.createElement("div");
        empty.textContent = "אין נתונים";
        trendHost.appendChild(empty);
        return;
    }

    const ns = "http://www.w3.org/2000/svg";
    const width = 780;
    const height = 320;
    const margin = { top: 20, right: 20, bottom: 58, left: 52 };
    const plotW = width - margin.left - margin.right;
    const plotH = height - margin.top - margin.bottom;

    const svgEl = document.createElementNS(ns, "svg");
    svgEl.setAttribute("width", "100%");
    svgEl.setAttribute("viewBox", "0 0 " + width + " " + height);

    const yMin = 0;
    const yMax = 100;

    function toY(v) {
        const n = Number(v);
        const clamped = Number.isFinite(n) ? Math.max(yMin, Math.min(yMax, n)) : yMin;
        const t = (clamped - yMin) / (yMax - yMin);
        return margin.top + (1 - t) * plotH;
    }

    const axisX = document.createElementNS(ns, "line");
    axisX.setAttribute("x1", String(margin.left));
    axisX.setAttribute("y1", String(margin.top + plotH));
    axisX.setAttribute("x2", String(margin.left + plotW));
    axisX.setAttribute("y2", String(margin.top + plotH));
    axisX.setAttribute("stroke", "#444");
    svgEl.appendChild(axisX);

    const axisY = document.createElementNS(ns, "line");
    axisY.setAttribute("x1", String(margin.left));
    axisY.setAttribute("y1", String(margin.top));
    axisY.setAttribute("x2", String(margin.left));
    axisY.setAttribute("y2", String(margin.top + plotH));
    axisY.setAttribute("stroke", "#444");
    svgEl.appendChild(axisY);

    [0, 25, 50, 75, 100].forEach(tick => {
        const y = toY(tick);
        const grid = document.createElementNS(ns, "line");
        grid.setAttribute("x1", String(margin.left));
        grid.setAttribute("y1", String(y));
        grid.setAttribute("x2", String(margin.left + plotW));
        grid.setAttribute("y2", String(y));
        grid.setAttribute("stroke", "#e8e8e8");
        svgEl.appendChild(grid);

        const label = document.createElementNS(ns, "text");
        label.textContent = tick + "%";
        label.setAttribute("x", String(margin.left - 35));
        label.setAttribute("y", String(y + 4));
        label.setAttribute("text-anchor", "end");
        label.setAttribute("font-size", "11");
        label.setAttribute("fill", "#444");
        svgEl.appendChild(label);
    });

    const xStep = points.length > 1 ? (plotW / (points.length - 1)) : 0;
    function toX(i) {
        return margin.left + (i * xStep);
    }

    let pathData = "";
    points.forEach((p, i) => {
        const x = toX(i);
        const y = toY(p.perc);
        pathData += (i === 0 ? "M" : " L") + x + " " + y;
    });

    const line = document.createElementNS(ns, "path");
    line.setAttribute("d", pathData);
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", "#0b5fff");
    line.setAttribute("stroke-width", "2");
    svgEl.appendChild(line);

    points.forEach((p, i) => {
        const x = toX(i);
        const y = toY(p.perc);
        const monthNum = parseInt(p.month, 10);
        const ymLabel = String(p.year) + "-" + String(monthNum).padStart(2, "0");

        const dot = document.createElementNS(ns, "circle");
        dot.setAttribute("cx", String(x));
        dot.setAttribute("cy", String(y));
        dot.setAttribute("r", "3.5");
        dot.setAttribute("fill", "#0b5fff");
        const title = document.createElementNS(ns, "title");
        title.textContent = ymLabel + ": " + String(p.perc) + "%";
        dot.appendChild(title);
        svgEl.appendChild(dot);

        const xLabel = document.createElementNS(ns, "text");
        xLabel.textContent = ymLabel;
        xLabel.setAttribute("x", String(x));
        xLabel.setAttribute("y", String(margin.top + plotH + 20));
        xLabel.setAttribute("text-anchor", "middle");
        xLabel.setAttribute("font-size", "11");
        xLabel.setAttribute("fill", "#333");
        svgEl.appendChild(xLabel);
    });

    trendHost.appendChild(svgEl);
}

function openStationTrendModal() {
    if (!trendModal || !trendHost || !trendTitle) return;

    const rows = BUS_TO_RAIL_TREND_JSON;

    trendTitle.textContent = "אחוזי סנכרון עבור תחנת " + station;
    renderStationTrendChart(rows);
    trendModal.classList.add("open");
}

if (openStationTrendBtn) {
    openStationTrendBtn.addEventListener("click", () => {
        openStationTrendModal();
        closeManualSyncPopups();
    });
}


// endregion percentage graph STATION LEVEL


// region Bus Table

let activeSignage = null;

const KEY_SIGNAGE = "שילוט";
function getSignageValue(row) {
    const raw = row[KEY_SIGNAGE];
    if (raw === null || raw === undefined || String(raw).trim() === "") return "";
    const n = Number(raw);
    if (Number.isFinite(n)) return String(Math.trunc(n));
    return String(raw).trim();
}

const KEY_MAKAT = 'מק"ט';
function getMakatValue(row) {
    const raw = row[KEY_MAKAT];
    if (raw === null || raw === undefined || String(raw).trim() === "") return "";
    const n = Number(raw);
    if (Number.isFinite(n)) return String(Math.trunc(n));
    return String(raw).trim();
}

const KEY_TRAIN_ID = "מספר הרכבת";
function getTrainIdValue(row) {
    const raw = row[KEY_TRAIN_ID];
    if (raw === null || raw === undefined || String(raw).trim() === "") return "";
    const n = Number(raw);
    if (Number.isFinite(n)) return String(Math.trunc(n));
    return String(raw).trim();
}

const KEY_TRAIN_ARRIVAL_RISHUI = "שעת הגעת הרכבת לתחנה (רישוי)";
const KEY_ASCENDING_AMOUNT = "מספר עולים";
const KEY_DESCENDING_AMOUNT = "מספר יורדים";
const KEY_DIRECTION = "כיוון";
const KEY_ALTERNATIVE = "חלופה";
const KEY_BUS_DEPARTURE_TIME = "שעת יציאה מתחנת המוצא";
const KEY_AVG_PASSENGERS = "ממוצע נוסעים לנסיעה";
const KEY_BUS_TIME_TO_STATION = "שעת הגעה לתחנה (בממוצע)";
const KEY_ARRIVAL_TIME_WINDOW = "סטיית תקן משעת ההגעה לתחנה";
function getArrivalWindowInt(row) {
    const n = Number(row[KEY_ARRIVAL_TIME_WINDOW]);
    return Number.isFinite(n) ? Math.round(n) : null;
}
const KEY_MINUTES_GAP_B2R = "הפרש בדקות (מאוטובוס לרכבת)";
const KEY_RECOMMENDED_MINUTES = "המלצה (דקות)";
const KEY_ON_TIME_PERCENT = "אחוז הנסיעות שעמדו בזמנים";
const KEY_IS_BUS_ON_TIME = "האם האוטובוס מגיע בזמן";
const KEY_DURATION_FROM_HASHALOM = "זמן נסיעת הרכבת מתחנת רכבת השלום (דקות)";
const PERC_ON_TIME = "אחוז הנסיעות שעמדו בזמנים (סימולציה)"; //from the Raw Bus Data json
const TABLE_FIELDS_BUS_TO_RAIL = [
    { key: KEY_TRAIN_ID, label: "מספר הרכבת" },
    { key: KEY_TRAIN_ARRIVAL_RISHUI, label: "שעת הגעת הרכבת לתחנה (רישוי)" },
    { key: KEY_DURATION_FROM_HASHALOM, label: "זמן נסיעת הרכבת מתחנת רכבת השלום (דקות)" },
    { key: KEY_ASCENDING_AMOUNT, label: "מספר עולים לרכבת" },
    { key: KEY_OPERATOR, label: "מפעיל" },
    { key: KEY_MAKAT, label: 'מק"ט' },
    { key: KEY_DIRECTION, label: "כיוון" },
    { key: KEY_ALTERNATIVE, label: "חלופה" },
    { key: KEY_BUS_DEPARTURE_TIME, label: "שעת יציאה מתחנת המוצא" },
    { key: KEY_AVG_PASSENGERS, label: "ממוצע נוסעים לנסיעה" },
    { key: KEY_BUS_TIME_TO_STATION, label: "שעת הגעה לתחנה (בממוצע)" },
    { key: KEY_ARRIVAL_TIME_WINDOW,label: "סטיית תקן משעת ההגעה לתחנה"},
    { key: KEY_MINUTES_GAP_B2R, label: "הפרש בדקות (מאוטובוס לרכבת)" },
    { key: KEY_RECOMMENDED_MINUTES, label: "המלצה בדקות" },
    { key: KEY_ON_TIME_PERCENT, label: "אחוז הנסיעות שעמדו בזמנים" },
];

const SIMULATION_TABLE_FIELDS_BUS_TO_RAIL = [
    { key: KEY_TRAIN_ID, label: "מספר הרכבת" },
    { key: KEY_TRAIN_ARRIVAL_RISHUI, label: "שעת הגעת הרכבת לתחנה (רישוי)" },
    { key: KEY_DURATION_FROM_HASHALOM, label: "זמן נסיעת הרכבת מתחנת רכבת השלום (דקות)" },
    { key: KEY_ASCENDING_AMOUNT, label: "מספר עולים לרכבת" },
    { key: KEY_MAKAT, label: 'מק"ט' },
    { key: KEY_DIRECTION, label: "כיוון" },
    { key: KEY_ALTERNATIVE, label: "חלופה" },
    { key: KEY_BUS_DEPARTURE_TIME, label: "שעת יציאה מתחנת המוצא" },
    { key: KEY_BUS_TIME_TO_STATION, label: "שעת הגעה לתחנה (בממוצע)" },
    { key: KEY_MINUTES_GAP_B2R, label: "הפרש בדקות (מאוטובוס לרכבת)" },
    { key: KEY_RECOMMENDED_MINUTES, label: "המלצה בדקות" },
    { key: PERC_ON_TIME, label: PERC_ON_TIME },
];


const KEY_MINUTES_GAP_R2B = "הפרש בדקות (מרכבת לאוטובוס)";
const KEY_DURATION_TO_HASHALOM = "זמן נסיעת הרכבת לתחנת רכבת השלום (דקות)";

const TABLE_FIELDS_RAIL_TO_BUS = [
    { key: KEY_TRAIN_ID, label: "מספר הרכבת" },
    { key: KEY_TRAIN_ARRIVAL_RISHUI, label: "שעת הגעת הרכבת לתחנה (רישוי)" },
    { key: KEY_DURATION_TO_HASHALOM, label: "זמן נסיעת הרכבת לתחנת רכבת השלום (דקות)" },
    { key: KEY_DESCENDING_AMOUNT, label: "מספר יורדים מהרכבת" },
    { key: KEY_OPERATOR, label: "מפעיל" },
    { key: KEY_MAKAT, label: 'מק"ט' },
    { key: KEY_DIRECTION, label: "כיוון" },
    { key: KEY_ALTERNATIVE, label: "חלופה" },
    { key: KEY_BUS_DEPARTURE_TIME, label: "שעת יציאה מתחנת המוצא" },
    { key: KEY_AVG_PASSENGERS, label: "ממוצע נוסעים לנסיעה" },
    { key: KEY_MINUTES_GAP_R2B, label: "הפרש בדקות (מרכבת לאוטובוס)" },
    { key: KEY_RECOMMENDED_MINUTES, label: "המלצה בדקות" },
];

const SIMULATION_TABLE_FIELDS_RAIL_TO_BUS = [
    { key: KEY_TRAIN_ID, label: "מספר הרכבת" },
    { key: KEY_TRAIN_ARRIVAL_RISHUI, label: "שעת הגעת הרכבת לתחנה (רישוי)" },
    { key: KEY_DURATION_TO_HASHALOM, label: "זמן נסיעת הרכבת לתחנת רכבת השלום (דקות)" },
    { key: KEY_DESCENDING_AMOUNT, label: "מספר יורדים מהרכבת" },
    { key: KEY_MAKAT, label: 'מק"ט' },
    { key: KEY_DIRECTION, label: "כיוון" },
    { key: KEY_ALTERNATIVE, label: "חלופה" },
    { key: KEY_BUS_DEPARTURE_TIME, label: "שעת יציאה מתחנת המוצא" },
    { key: KEY_MINUTES_GAP_R2B, label: "הפרש בדקות (מרכבת לאוטובוס)" },
    { key: KEY_RECOMMENDED_MINUTES, label: "המלצה בדקות" },
];


// Defines which fields appear in the table and in what order
function getTableFields() {
    return (activeDirection === BUS_TO_RAIL_DIRECTION)
      ? {
          main: TABLE_FIELDS_BUS_TO_RAIL,
          simulation: SIMULATION_TABLE_FIELDS_BUS_TO_RAIL,
        }
      : {
          main: TABLE_FIELDS_RAIL_TO_BUS,
          simulation: SIMULATION_TABLE_FIELDS_RAIL_TO_BUS,
        };
}

let activeBusRowKey = null;
function buildBusRowKey(row) {
    const trainId = getTrainIdValue(row);
    const makat = getMakatValue(row);
    const direction = String(row[KEY_DIRECTION] ?? "").trim();
    const alternative = String(row[KEY_ALTERNATIVE] ?? "").trim();
    const departure = extractHHMM(row[KEY_BUS_DEPARTURE_TIME] ?? "") || "";
    return trainId + "||" + makat + "||" + direction + "||" + alternative + "||" + departure;
  }

function setActiveBusRow(rowOrNull) {
    activeBusRowKey = rowOrNull ? buildBusRowKey(rowOrNull) : null;
    renderBusTable();
}


function updateTopTableScrollbar(ids = {
    top: "busTableTopScroll",
    topInner: "busTableTopScrollInner",
    main: "busTableMainScroll",
    table: "busTable",
    }) {
    const top = document.getElementById(ids.top);
    const topInner = document.getElementById(ids.topInner);
    const main = document.getElementById(ids.main);
    const table = document.getElementById(ids.table);
    if (!top || !topInner || !main || !table) return;

    const width = table.scrollWidth;
    topInner.style.width = width + "px";
    top.style.display = width > main.clientWidth ? "block" : "none";
}

const tableScrollSyncBound = {};

function bindTableScrollSync(ids = {
    top: "busTableTopScroll",
    main: "busTableMainScroll",
    topInner: "busTableTopScrollInner",
    table: "busTable",
    }) {
    const bindKey = ids.top + "|" + ids.main;
    if (tableScrollSyncBound[bindKey]) return;

    const top = document.getElementById(ids.top);
    const main = document.getElementById(ids.main);
    if (!top || !main) return;

    let syncingTop = false;
    let syncingMain = false;

    top.addEventListener("scroll", () => {
      if (syncingMain) return;
      syncingTop = true;
      main.scrollLeft = top.scrollLeft;
      syncingTop = false;
    });

    main.addEventListener("scroll", () => {
      if (syncingTop) return;
      syncingMain = true;
      top.scrollLeft = main.scrollLeft;
      syncingMain = false;
    });

    window.addEventListener("resize", () => updateTopTableScrollbar(ids));
    tableScrollSyncBound[bindKey] = true;
}

function alignTableScrollToRight(ids = {
    top: "busTableTopScroll",
    main: "busTableMainScroll",
  }) {
    const top = document.getElementById(ids.top);
    const main = document.getElementById(ids.main);
    if (!top || !main) return;

    const maxLeft = Math.max(0, main.scrollWidth - main.clientWidth);
    main.scrollLeft = maxLeft;
    top.scrollLeft = maxLeft;
  }

function renderBusTable() {
  const headerRow = document.getElementById("busTableHeaderRow");
  const body = document.getElementById("busTableBody");
  if (!headerRow || !body) return;


  const tableFields = getTableFields().main;

  // Build header
  headerRow.innerHTML = "";
  // header
  tableFields.forEach((f, idx) => {
    const th = document.createElement("th");
    th.textContent = f.label;
    if (idx < 4) th.classList.add("train-highlight-col");
    headerRow.appendChild(th);
  });

  // Choose which rows to show
  const allRows = getFilteredTimes();

    // Read visible bus keys from map
  const visibleBusKeys = new Set(
    Array.from(svg.querySelectorAll(".bus-item"))
      .filter(el => el.style.display !== "none")
      .map(el => el.getAttribute("data-bus-key") || "")
      .filter(Boolean)
  );

  // Table rows follow visible dots exactly
  const rows = allRows.filter(r => visibleBusKeys.has(buildBusRowKey(r)));


  // Build body
  body.innerHTML = "";

  rows.forEach(r => {
    const tr = document.createElement("tr");
    const rowBusKey = buildBusRowKey(r);
    tr.setAttribute("data-bus-key", rowBusKey);
    tableFields.forEach((f, idx) => {
      const td = document.createElement("td");
      if (idx < 4) td.classList.add("train-highlight-col");

      let v = "";

      v = r[f.key];
      if (f.key === KEY_ARRIVAL_TIME_WINDOW) {
        const intVal = getArrivalWindowInt(r);
        v = (intVal === null) ? "—" : `${intVal} דק`;
      }
      if (f.key === KEY_ON_TIME_PERCENT) {
        const rawPerc = String(v ?? "").trim();
        const normalizedPerc = rawPerc.replace("%", "").replace(",", ".");
        const percNum = Number(normalizedPerc);
        v = Number.isFinite(percNum) ? `${Math.trunc(percNum)}%` : "—";
      }
      if (v === null || v === undefined || String(v).trim() === "") {
        v = "—";
      } else {
        // If the label contains "שעת", try to normalize to HH:MM
        if (String(f.label).includes("שעת")) {
          v = extractHHMM(v) || String(v).trim();
        } else {
          v = String(v).trim();
        }
      }

      if (f.key === "\u05de\u05e7\"\u05d8" && v !== "—") {
        const link = document.createElement("a");
        link.href = "https://markav.net/line/" + encodeURIComponent(v);
        link.target = "_blank";
        link.rel = "noopener noreferrer";
        link.textContent = v;
        td.appendChild(link);
      } else {
        td.textContent = v;
      }
      if (f.key === "המלצה (דקות)") {
        td.style.direction = "ltr";
        td.style.unicodeBidi = "plaintext";
      }
      if (f.key === "הפרש בדקות (מאוטובוס לרכבת)") {
        td.style.direction = "ltr";
        td.style.unicodeBidi = "plaintext";
      }
      tr.appendChild(td);
    });
    tr.style.cursor = "pointer";
    tr.addEventListener("click", (e) => {
        e.stopPropagation();

        const isSame = (activeBusRowKey === rowBusKey);

        if (isSame) {
          activeBusRowKey = null;
          setActiveTrain(null); // clear train highlight/emoji
        } else {
          activeBusRowKey = rowBusKey;

          // row click should ignore signage filter
          activeSignage = null;
          updateBusHeaderSelectionVisuals();

          const connectedTrainUid = getConnectedTrainUidForBusRow(r);
          setActiveTrain(connectedTrainUid); // show related train
        }

        applyBusFilter();
        renderBusTable();
        closeManualSyncPopups();
  });
    body.appendChild(tr);
  });

  bindTableScrollSync();
  updateTopTableScrollbar();
  alignTableScrollToRight();
}
// endregion Bus Table


// region simulation

// data for perc simulation columns:
const RAW_BUS_DATA_JSON = JSON.parse(
    document.getElementById("raw-bus-data").textContent
  );

const RAW_KEY_MAKAT = "makat";
const RAW_KEY_DIRECTION = "direction";
const RAW_KEY_ALTERNATIVE = "alternative";
const RAW_KEY_DEPARTURE = "departure_time";
const RAW_KEY_ARRIVAL_TIME = "bus_arrival_time_to_station";
const RAW_KEY_RIDE_COUNTS = "ride_counts";


let filteredRawBusData = [];

function rebuildFilteredRawBusData() {
    const week = String(activeWeek ?? "").trim();
    const railDir = String(activeDirection ?? "").trim();

    filteredRawBusData = RAW_BUS_DATA_JSON.filter((r) => {
      const rowWeek = String(r.week_period ?? "").trim();
      const rowRailDir = String(r.rail_direction ?? "").trim();

      if (week && rowWeek !== week) return false;
      if (railDir && rowRailDir && rowRailDir !== railDir) return false;

      return true;
    });
}

function hhmmToMinutes(v) {
    const t = extractHHMM(v || "");
    if (!t || !t.includes(":")) return null;
    const [h, m] = t.split(":").map(Number);
    if (!Number.isFinite(h) || !Number.isFinite(m)) return null;
    return h * 60 + m;
}

// end - start in minutes, normalized across midnight
function diffMinutes(startVal, endVal) {
    const s = hhmmToMinutes(startVal);
    const e = hhmmToMinutes(endVal);
    if (s === null || e === null) return null;
    let d = e - s;
    if (d < -720) d += 1440;
    if (d > 720) d -= 1440;
    return d;
}

function getRecommendedMinutesFromGap(gapValue) {
    const gap = Number(gapValue);
    if (!Number.isFinite(gap)) return "";

    // If the gap is already between 8 and 15, do nothing
    if (gap >= 8 && gap <= 15) {
        return "—";
    }

    const targetGap = 15;

    const rawDelta = targetGap - gap;

    const roundedDelta = Math.round(rawDelta / 5) * 5;

    const flippedDelta = -roundedDelta;

    return flippedDelta >= 0 ? `+${flippedDelta}` : `${flippedDelta}`;
}



function getMatchedRawRowsForSimulationSource() {
    if (!simulationSourceBusRow) return [];

    const makat = String(getMakatValue(simulationSourceBusRow) ?? "").trim();
    const dir = String(simulationSourceBusRow[KEY_DIRECTION] ?? "").trim();
    const alt = String(simulationSourceBusRow[KEY_ALTERNATIVE] ?? "").trim();
    const dep = extractHHMM(simulationSourceBusRow[KEY_BUS_DEPARTURE_TIME] ?? "") ||
      String(simulationSourceBusRow[KEY_BUS_DEPARTURE_TIME] ?? "").trim();

    return filteredRawBusData.filter((r) => {
      const rMakat = String(r[RAW_KEY_MAKAT] ?? "").trim();
      const rDir = String(r[RAW_KEY_DIRECTION] ?? "").trim();
      const rAlt = String(r[RAW_KEY_ALTERNATIVE] ?? "").trim();
      const rDep = extractHHMM(r[RAW_KEY_DEPARTURE] ?? "") || String(r[RAW_KEY_DEPARTURE] ?? "").trim();

      const ok = (rMakat === makat && rDir === dir && rAlt === alt && rDep === dep);
      return ok;
    });
}




function calcSimulationOnTimePercent(trainArrivalRishui, rawRows) {
    const groups = new Map();

    rawRows.forEach((r) => {
      const makat = String(r[RAW_KEY_MAKAT] ?? "").trim();
      const direction = String(r[RAW_KEY_DIRECTION] ?? "").trim();
      const alternative = String(r[RAW_KEY_ALTERNATIVE] ?? "").trim();
      const departure = extractHHMM(r[RAW_KEY_DEPARTURE] ?? "") || String(r[RAW_KEY_DEPARTURE] ?? "").trim();

      const groupKey = [makat, direction, alternative, departure].join("|");

      const arr = String(r[RAW_KEY_ARRIVAL_TIME] ?? "").trim();
      const rides = Number(r[RAW_KEY_RIDE_COUNTS]);
      if (!Number.isFinite(rides) || rides <= 0) return;

      const arrMin = hhmmToMinutes(arr);
      const trainMin = hhmmToMinutes(trainArrivalRishui);
      if (!Number.isFinite(arrMin) || !Number.isFinite(trainMin)) return;

      const gap = trainMin - arrMin;
      const isTrue = gap >= 8 && gap <= 15;

      if (!groups.has(groupKey)) {
        groups.set(groupKey, {
          rides,      // counted once per group
          goodCases: 0
        });
      }

      if (isTrue) {
        groups.get(groupKey).goodCases += 1;
      }
    });

    let trueCount = 0;
    let denominator = 0;

    groups.forEach((group) => {
      trueCount += group.goodCases;
      denominator += group.rides;
    });

    if (denominator <= 0) return "";

    const pct = (trueCount / denominator) * 100;
    return `${pct.toFixed(1)}%`;
}

rebuildFilteredRawBusData(); //initial load




let simulationCaptureActive = false;
const simulationRows = []; // rows shown in simulation table

function renderSimulationBusTable() {
    const headerRow = document.getElementById("simulationTableHeaderRow");
    const body = document.getElementById("simulationTableBody");
    const wrap = document.getElementById("simulationTableWrap");
    if (!headerRow || !body || !wrap) return;

    const tableFields = getTableFields().simulation;

    // Header
    headerRow.innerHTML = "";
    tableFields.forEach((f, idx) => {
        const th = document.createElement("th");
        th.textContent = f.label;

        if (idx < 4) {
          th.classList.add("train-highlight-col");
        }

        if (
          f.key === KEY_MINUTES_GAP_B2R ||
          f.key === KEY_MINUTES_GAP_R2B ||
          f.key === KEY_RECOMMENDED_MINUTES
        ) {
            th.classList.add("sim-warning-col");
        }

        headerRow.appendChild(th);
    });

    // Body from captured simulation rows only
    const rows = simulationRows;
    body.innerHTML = "";

    rows.forEach((r) => {
      const tr = document.createElement("tr");

      tableFields.forEach((f, idx) => {
        const td = document.createElement("td");
        if (idx < 4) td.classList.add("train-highlight-col");
        if (
          f.key === KEY_MINUTES_GAP_B2R ||
          f.key === KEY_MINUTES_GAP_R2B ||
          f.key === KEY_RECOMMENDED_MINUTES
        ) {
            td.style.direction = "ltr";
            td.style.unicodeBidi = "plaintext";
        }

        let v = r[f.key];

        if (v === null || v === undefined || String(v).trim() === "") {
          v = "—";
        } else if (String(f.label).includes("שעת")) {
          v = extractHHMM(v) || String(v).trim();
        } else {
          v = String(v).trim();
        }

        td.textContent = v;
        tr.appendChild(td);
      });

      body.appendChild(tr);
    });

    wrap.style.display = "block";

    const simIds = {
      top: "simulationTableTopScroll",
      topInner: "simulationTableTopScrollInner",
      main: "simulationTableMainScroll",
      table: "simulationTable",
    };
    bindTableScrollSync(simIds);
    updateTopTableScrollbar(simIds);
    alignTableScrollToRight(simIds);
}

const simulationCapturedTrainUids = new Set();

function captureSimulationRowFromTrain(row, trainUid, triEl) {
    if (!simulationCaptureActive) return false;
    if (!simulationSourceBusRow) return false;

    let simRow;
    if (activeDirection === BUS_TO_RAIL_DIRECTION) {
      const gap = diffMinutes(
        simulationSourceBusRow[KEY_BUS_TIME_TO_STATION], // train_arrival_rishui - bus_time_to_station
        row[KEY_TRAIN_ARRIVAL_RISHUI]
      );
      simRow = {
        [KEY_TRAIN_ID]: getTrainIdValue(row),
        [KEY_TRAIN_ARRIVAL_RISHUI]: extractHHMM(row[KEY_TRAIN_ARRIVAL_RISHUI] || "") || String(row[KEY_TRAIN_ARRIVAL_RISHUI] || "").trim(),
        [KEY_DURATION_FROM_HASHALOM]: row[KEY_DURATION_FROM_HASHALOM],
        [KEY_ASCENDING_AMOUNT]: row[KEY_ASCENDING_AMOUNT],
        [KEY_MAKAT]: getMakatValue(simulationSourceBusRow),
        [KEY_DIRECTION]: simulationSourceBusRow[KEY_DIRECTION],
        [KEY_ALTERNATIVE]: simulationSourceBusRow[KEY_ALTERNATIVE],
        [KEY_BUS_DEPARTURE_TIME]: simulationSourceBusRow[KEY_BUS_DEPARTURE_TIME],
        [KEY_BUS_TIME_TO_STATION]: simulationSourceBusRow[KEY_BUS_TIME_TO_STATION],
        [KEY_MINUTES_GAP_B2R]: gap,
        [KEY_RECOMMENDED_MINUTES]: getRecommendedMinutesFromGap(gap),
      };
      const matchedRawRows = getMatchedRawRowsForSimulationSource();
      simRow[PERC_ON_TIME] = calcSimulationOnTimePercent(
        row[KEY_TRAIN_ARRIVAL_RISHUI],
        matchedRawRows
      );
    } else {
      const gap = diffMinutes(
        row[KEY_TRAIN_ARRIVAL_RISHUI],
        simulationSourceBusRow[KEY_BUS_DEPARTURE_TIME]
      );
      simRow = {
        [KEY_TRAIN_ID]: getTrainIdValue(row),
        [KEY_TRAIN_ARRIVAL_RISHUI]: extractHHMM(row[KEY_TRAIN_ARRIVAL_RISHUI] || "") || String(row[KEY_TRAIN_ARRIVAL_RISHUI] || "").trim(),
        [KEY_DURATION_TO_HASHALOM]: row[KEY_DURATION_TO_HASHALOM],
        [KEY_DESCENDING_AMOUNT]: row[KEY_DESCENDING_AMOUNT],
        [KEY_MAKAT]: getMakatValue(simulationSourceBusRow),
        [KEY_DIRECTION]: simulationSourceBusRow[KEY_DIRECTION],
        [KEY_ALTERNATIVE]: simulationSourceBusRow[KEY_ALTERNATIVE],
        [KEY_BUS_DEPARTURE_TIME]: simulationSourceBusRow[KEY_BUS_DEPARTURE_TIME],
        [KEY_BUS_TIME_TO_STATION]: simulationSourceBusRow[KEY_BUS_TIME_TO_STATION],
        [KEY_MINUTES_GAP_R2B]: gap,
        [KEY_RECOMMENDED_MINUTES]: getRecommendedMinutesFromGap(gap),
      };
    }

    simulationRows.push(simRow);
    simulationCapturedTrainUids.add(trainUid);
    triEl.setAttribute("fill", "#ff8c00");
    triEl.setAttribute("opacity", "1");
    renderSimulationBusTable();
    return true;
}


function resetSimulationCapture() {
    simulationCaptureActive = false;
    simulationSourceBusRow = null;
    simulationRows.length = 0;
    simulationCapturedTrainUids.clear();
    renderSimulationBusTable(); // empty table
    redrawTrains(); // restores default train colors
    const wrap = document.getElementById("simulationTableWrap");
    if (wrap) wrap.style.display = "none";
}

// endregion simulation

// region Map - Trains

let activeTrainUid = null;


const KEY_IS_GOLD_TRAIN = "רכבת זהב";
const KEY_IS_EXPRESS_TRAIN = "סוג רכבת";

const svg = document.getElementById("arrowSVG");


function applyBusFilter() {
    const buses = svg.querySelectorAll(".bus-item");

    buses.forEach(b => {
      const uid = b.getAttribute("data-train-uid") || "";
      const signage = b.getAttribute("data-signage") || "";
      const busKey = b.getAttribute("data-bus-key") || "";

      const visible =
        activeTrainUid ? (uid === activeTrainUid) :
        activeSignage ? (signage === activeSignage) :
        true;

      const busRowOk = (!activeBusRowKey || busKey === activeBusRowKey);

      b.style.display = (visible && busRowOk) ? "" : "none";
    });


  }

function applyPercFilter() {
  const percs = svg.querySelectorAll(".perc-item");
  percs.forEach(p => {
      const uid = p.getAttribute("data-train-uid") || "";
      p.style.display = (activeTrainUid && uid === activeTrainUid) ? "" : "none";
  });
}

function clearTrainOutlines() {
    const trains = svg.querySelectorAll(".train-item");
    trains.forEach(t => {
        t.setAttribute("opacity", "0.5");
    });
}

function setActiveTrain(trainUid = null) {
    activeTrainUid = trainUid ? String(trainUid) : null;

    // train selection cancels signage selection
    if (activeTrainUid) activeSignage = null;

    applyBusFilter();
    applyPercFilter();

    clearTrainOutlines();
    if (activeTrainUid) {
      const selector = '.train-item[data-train-uid="' + activeTrainUid + '"]';
      const selected = svg.querySelectorAll(selector);
      selected.forEach(t => {
          t.setAttribute("opacity", "1");
      });
    }
      // emoji visibility control
    svg.querySelectorAll(".train-trend-emoji").forEach(el => {
      const uid = String(el.getAttribute("data-train-uid") || "");
      el.style.display = (activeTrainUid && uid === activeTrainUid) ? "" : "none";
    });
    renderBusTable();
    updateBusHeaderSelectionVisuals();
}


function redrawTrains() {
    // deleting old traingles and percentages:
    svg.querySelectorAll(".train-item, .perc-item, .express-item, .train-trend-emoji").forEach(el => el.remove());
    //

    // building a unique trainId||time (for later traingles creation)
    const filteredTimes = getFilteredTimes();
    const seenTrainTime = new Set();
    const uniqueTrainRows = [];

    filteredTimes.forEach(row => {
        const rawTime = row[KEY_TRAIN_ARRIVAL_RISHUI];
        const normalizedTime = extractHHMM(rawTime);
        if (!normalizedTime) return;

        const y = timeToY(normalizedTime);
        if (y === null) return;

        const trainId = getTrainIdValue(row);
        if (!trainId) return;

        const trainUid = trainId + "||" + normalizedTime;
        if (seenTrainTime.has(trainUid)) return;
        seenTrainTime.add(trainUid);

        uniqueTrainRows.push({
            row,
            normalizedTime,
            y,
            trainId,
            trainUid,
            xOffset: 0
        });
    });

    //

    // Jitter only unique train-time items that are closer than 4 minutes.
    const pxPerMinute = (1920 - 40) / (24 * 60);
    const jitterThresholdPx = 4 * pxPerMinute;
    const jitterStepX = 10;

    uniqueTrainRows.sort((a, b) => a.y - b.y);

    let i = 0;
    while (i < uniqueTrainRows.length) {
        let j = i + 1;
        while (j < uniqueTrainRows.length && (uniqueTrainRows[j].y - uniqueTrainRows[j - 1].y) < jitterThresholdPx) {
            j += 1;
        }

        const clusterSize = j - i;
        const startX = -((clusterSize - 1) * jitterStepX) / 2;
        for (let k = 0; k < clusterSize; k += 1) {
            uniqueTrainRows[i + k].xOffset = startX + (k * jitterStepX);
        }
        i = j;
    }
    //

    uniqueTrainRows.forEach(item => {
    const { row, normalizedTime, y, trainId, trainUid, xOffset } = item;
        // the train percentage label creation (only when BUS_TO_RAIL_DIRECTION)
        if (activeDirection === BUS_TO_RAIL_DIRECTION) {
            const percStr = row[KEY_ON_TIME_PERCENT_BY_TRAIN];
            if (percStr !== undefined && percStr !== null && String(percStr).trim() !== "") {
                const percText = document.createElementNS("http://www.w3.org/2000/svg", "text");

                percText.setAttribute("x", String(110 + xOffset));
                percText.setAttribute("y", String(y));
                percText.setAttribute("font-size", "12");
                percText.setAttribute("fill", "#333");
                percText.setAttribute("text-anchor", "end");
                percText.setAttribute("dominant-baseline", "middle");

                percText.setAttribute("direction", "ltr");
                percText.setAttribute("unicode-bidi", "plaintext");

                const rawPerc = String(percStr).trim();
                const normalizedPerc = rawPerc.replace("%", "").replace(",", ".");
                const percNum = Number(normalizedPerc);
                percText.textContent = Number.isFinite(percNum) ? `${Math.trunc(percNum)}%` : "";

                const pTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
                pTitle.textContent = "אחוז הנסיעות המסונכרנות";
                percText.appendChild(pTitle);
                percText.classList.add("perc-item");
                percText.setAttribute("data-train-uid", trainUid);
                percText.style.display = "none";

                svg.appendChild(percText);
            }
        }
        //


        // train color and gold train creation
        const isGold = String(row[KEY_IS_GOLD_TRAIN] ?? "").trim() === "כן";
        const color = isGold ? "gold" : "black";
        //

                //creating the sqaures:
        const tri = document.createElementNS("http://www.w3.org/2000/svg", "rect");
        const size = 12;

        const x = 120 + xOffset;
        const yPos = y - size / 2;

        tri.setAttribute("rx", "6");
        tri.setAttribute("x", String(x - size / 2));
        tri.setAttribute("y", String(yPos));
        tri.setAttribute("width", String(size));
        tri.setAttribute("height", String(size));
        tri.setAttribute("fill", color);
        tri.setAttribute("opacity", "0.5");
        tri.classList.add("train-item");
        tri.setAttribute("data-train-uid", trainUid);
        //



        // creating the hovering information on the circles:
        const trainTimeStr = normalizedTime;
        let paxLabel = "";
        let paxValue = null;
        if (activeDirection === BUS_TO_RAIL_DIRECTION) {
            paxLabel = KEY_ASCENDING_AMOUNT;
            paxValue = row[KEY_ASCENDING_AMOUNT];
        } else if (activeDirection === RAIL_TO_BUS_DIRECTION) {
            paxLabel = KEY_DESCENDING_AMOUNT;
            paxValue = row[KEY_DESCENDING_AMOUNT];
        }
        const paxText = (paxValue === null || paxValue === undefined || String(paxValue).trim() === "")
          ? "—"
          : String(paxValue).trim();

        const tTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
        tTitle.textContent = "זמן: " + trainTimeStr + " | רכבת: " + trainId + " | " + paxLabel + ": " + paxText;
        tri.appendChild(tTitle);
        //

        svg.appendChild(tri);
        // create trend emoji (same logic/text you already use in renderStationTrendChart)
        const trendEmoji = document.createElementNS("http://www.w3.org/2000/svg", "text");
        trendEmoji.textContent = "??";
        const trendTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
        trendTitle.textContent = "גרף אחוזי סנכרון";
        trendEmoji.appendChild(trendTitle);
        trendEmoji.style.display = "none";
        trendEmoji.setAttribute("x", String(x + size / 2 + 20)); // right side of train
        trendEmoji.setAttribute("y", String(y + 4));
        trendEmoji.setAttribute("font-size", "10");
        trendEmoji.style.cursor = "pointer";
        trendEmoji.classList.add("train-trend-emoji");
        trendEmoji.setAttribute("data-train-uid", trainUid);

        // open modal from emoji click (instead of from second train click)
        trendEmoji.addEventListener("click", (e) => {
            e.stopPropagation();
            closeManualSyncPopups();
            openTrainTrendModal(trainId, row);
        });

        svg.appendChild(trendEmoji);

        // single-click train only selects it
        tri.addEventListener("click", (e) => {
            e.stopPropagation();

            if (simulationCaptureActive && e.ctrlKey) {
                e.preventDefault();
                e.stopPropagation();
                captureSimulationRowFromTrain(row, trainUid, tri);
                return;
            }

            //cancek simulation effects:
            resetSimulationCapture();
            // cancel previous bus-dot selection
            activeBusRowKey = null;
            linkModeSourceRow = null;
            linkModeSourceBusKey = null;
            linkModeSourceDot = null;
            linkModeConnectedTrainUid = null;
            isLinkDragging = false;

            setActiveTrain(trainUid);
            svg.querySelectorAll(".train-trend-emoji").forEach(el => {
            el.style.display = "none";
            closeManualSyncPopups();
        });
            trendEmoji.style.display = "";
        });
        // train express
        const isExpress = String(row[KEY_IS_EXPRESS_TRAIN] ?? "").trim() === "אקספרס";
        if (isExpress) {
          const expressText = document.createElementNS("http://www.w3.org/2000/svg", "text");
          expressText.setAttribute("x", x);
          expressText.setAttribute("y", yPos + size * 0.75); // visually centered in train
          expressText.setAttribute("text-anchor", "middle");
          expressText.setAttribute("font-size", "7");
          expressText.setAttribute("font-weight", "700");
          expressText.setAttribute("fill", "#FFFFFF"); // white
          expressText.style.pointerEvents = "none"; // don't block train click
          expressText.classList.add("express-item");
          expressText.textContent = "E";
          svg.appendChild(expressText);
        }
        //
    });

}


// endregion Map - Trains


// region Map - Bus

function getBusTimeByDirection(row) {
    return (activeDirection === BUS_TO_RAIL_DIRECTION)
        ? row[KEY_BUS_TIME_TO_STATION]
        : row[KEY_BUS_DEPARTURE_TIME];
}

function getConnectedTrainUidForBusRow(row) {
    const trainId = getTrainIdValue(row);
    const trainTime = extractHHMM(row[KEY_TRAIN_ARRIVAL_RISHUI] || "") || String(row[KEY_TRAIN_ARRIVAL_RISHUI] || "").trim();
    if (!trainId || !trainTime) return "";
    return trainId + "||" + trainTime;
}

function hasLargeArrivalWindow(row) {
    if (activeDirection === RAIL_TO_BUS_DIRECTION) return false;
    const windowValue = getArrivalWindowInt(row);
    return windowValue !== null && windowValue >= 5;
}

function updateBusHeaderSelectionVisuals() {
    const headers = svg.querySelectorAll(".bus-header");
    headers.forEach(h => {
        const signage = h.getAttribute("data-signage") || "";
        const isActive = !!activeSignage && signage === activeSignage;

        h.setAttribute("fill", isActive ? "#0b5fff" : "#000");
        const bg = h.previousElementSibling;
        if (bg && bg.classList.contains("bus-header-bg")) {
            bg.setAttribute("fill", isActive ? "#e9f1ff" : "#fff");
            bg.setAttribute("stroke", isActive ? "#0b5fff" : "#ddd");
        }
    });
}

let busHeaderStickyBound = false;

function updateBusHeadersSticky() {
    const headers = svg.querySelectorAll(".bus-header");
    if (!headers.length) return;

    const svgRect = svg.getBoundingClientRect();
    const svgTopAbs = window.scrollY + svgRect.top;
    const baseY = 30;      // original header y in SVG
    const topPad = 8;      // distance from top while sticky
    const maxY = 1910;     // keep inside svg

    let desiredY = baseY;
    if (window.scrollY > svgTopAbs) {
      desiredY = baseY + (window.scrollY - svgTopAbs) + topPad;
    }

    desiredY = Math.min(desiredY, maxY);

    headers.forEach(h => {
        h.setAttribute("y", String(desiredY));

        const bg = h.previousElementSibling;
        if (bg && bg.classList.contains("bus-header-bg")) {
            const bb = h.getBBox();
            const padX = 6;
            const padY = 3;
            bg.setAttribute("x", String(bb.x - padX));
            bg.setAttribute("y", String(bb.y - padY));
            bg.setAttribute("width", String(bb.width + padX * 2));
            bg.setAttribute("height", String(bb.height + padY * 2));

            // Keep header layer above bus dots.
            svg.appendChild(bg);
            svg.appendChild(h);
        }
    });
}


let simulationSourceBusRow = null;

function redrawBuses() {
  //Removes previous bus dots, header text, and header backgrounds so you don’t duplicate them.
  svg.querySelectorAll(".bus-item, .bus-header, .bus-header-bg, .bus-header-title").forEach(el => el.remove());
  //

  // when resizing the page the stickey bus headers should resize accordingly
  if (!busHeaderStickyBound) {
    window.addEventListener("scroll", updateBusHeadersSticky, { passive: true });
    window.addEventListener("resize", updateBusHeadersSticky);
    busHeaderStickyBound = true;
  }
  updateBusHeadersSticky();

  // filtering the dots: :
  const filteredTimes = getFilteredTimes(); // using operator and week period
  const busRows = filteredTimes.filter(r =>
  getBusTimeByDirection(r) && getMakatValue(r) !== "" //  if there is not getBusTimeByDirection(r) && getMakatValue(r) filter it out!
  );

  // filtering the shilot header values - using only week period:
    // I do it so operator will not affect the values that are presented!
  const weekOnlyTimes = stationTimes.filter(r => String(r[KEY_WEEK_PERIOD] ?? "").trim() === activeWeek);
  const headerRows = weekOnlyTimes.filter(r =>
    getBusTimeByDirection(r) && getMakatValue(r) !== ""
  ); //  if there is not getBusTimeByDirection(r) && getMakatValue(r) filter it out!
  const shilotValues = Array.from(new Set(headerRows.map(r => getSignageValue(r))));

  // Sort signage: numeric signage in numeric order, then strings alphabetically
  shilotValues.sort((a, b) => {
    const na = parseFloat(a);
    const nb = parseFloat(b);
    const aIsNum = !Number.isNaN(na);
    const bIsNum = !Number.isNaN(nb);
    if (aIsNum && bIsNum) return na - nb;
    return a.localeCompare(b);
  });

  // X layout math for columns
  const baseX = 170;
  const colW  = 34;
  const rightPad = 20;

  // Resize SVG width if there are many signage columns
  const minSvgWidth = 240;
  const neededWidth = baseX + shilotValues.length * colW + rightPad;
  svg.setAttribute("width", String(Math.max(minSvgWidth, neededWidth)));

  // Map signage value -> x coordinate
  const shilotToX = new Map();
  shilotValues.forEach((val, idx) => {
    shilotToX.set(val, baseX + idx * colW);

    // title above bus signage headers
    const busHeadersTitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
    busHeadersTitle.classList.add("bus-header-title");
    busHeadersTitle.textContent = "קווי אוטובוס";
    busHeadersTitle.setAttribute("x", "200");
    busHeadersTitle.setAttribute("y", "10");
    busHeadersTitle.setAttribute("font-size", "12");
    busHeadersTitle.setAttribute("font-weight", "700");
    busHeadersTitle.setAttribute("fill", "#0b3a66");
    busHeadersTitle.setAttribute("text-anchor", "end");
    svg.appendChild(busHeadersTitle);
  });

  // Draw column headers at the top of SVG
  shilotValues.forEach(val => {
    const hx = shilotToX.get(val);

    const header = document.createElementNS("http://www.w3.org/2000/svg", "text");
    header.classList.add("bus-header");
    header.setAttribute("data-signage", String(val));
    header.setAttribute("x", hx);
    header.setAttribute("y", "30");
    header.setAttribute("font-size", "12");
    header.setAttribute("fill", "#000");
    header.setAttribute("font-weight", "700");
    header.setAttribute("text-anchor", "middle");
    header.setAttribute("direction", "ltr");
    header.setAttribute("unicode-bidi", "plaintext");
    header.style.cursor = "pointer";

    // White "pill" background around each header value
    const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    bg.classList.add("bus-header-bg");
    bg.setAttribute("data-signage", String(val));
    bg.setAttribute("rx", "4");
    bg.setAttribute("ry", "4");
    bg.setAttribute("fill", "#fff");
    bg.setAttribute("stroke", "#ddd");
    bg.setAttribute("stroke-width", "0.8");
    bg.style.cursor = "pointer";

    header.textContent = val;
    svg.appendChild(bg);
    svg.appendChild(header);

    // Must happen after header is in DOM so getBBox() is correct
    const bb = header.getBBox();
    const padX = 6;
    const padY = 3;
    bg.setAttribute("x", String(bb.x - padX));
    bg.setAttribute("y", String(bb.y - padY));
    bg.setAttribute("width", String(bb.width + padX * 2));
    bg.setAttribute("height", String(bb.height + padY * 2));

    const signageValue = String(val);

    // hovering over signates:
    const onHeaderMouseEnter = () => {
    if (activeSignage && activeSignage === signageValue) return;
    header.setAttribute("fill", "#0b5fff");
    bg.setAttribute("fill", "#f2f7ff");
    bg.setAttribute("stroke", "#8bb7ff");
    };

    const onHeaderMouseLeave = () => {
    if (activeSignage && activeSignage === signageValue) return;
    header.setAttribute("fill", "#000");
    bg.setAttribute("fill", "#fff");
    bg.setAttribute("stroke", "#ddd");
    };



    // clicking on signates:
    const onHeaderClick = (e) => {
      e.stopPropagation();
      activeSignage = (activeSignage === signageValue) ? null : signageValue;
      // signage selection cancels active train selection
      if (activeSignage) {
        activeTrainUid = null;
        clearTrainOutlines();
        applyPercFilter();
        svg.querySelectorAll(".train-trend-emoji").forEach(el => {
             el.style.display = "none";
    });
      }
      activeBusRowKey = null;

      updateBusHeaderSelectionVisuals();
      applyBusFilter();
      renderBusTable();
      closeManualSyncPopups();
      resetSimulationCapture();
    };
    header.addEventListener("click", onHeaderClick);
    bg.addEventListener("click", onHeaderClick);
    header.addEventListener("mouseenter", onHeaderMouseEnter);
    header.addEventListener("mouseleave", onHeaderMouseLeave);
    bg.addEventListener("mouseenter", onHeaderMouseEnter);
    bg.addEventListener("mouseleave", onHeaderMouseLeave);
  });

  updateBusHeadersSticky();
  updateBusHeaderSelectionVisuals();

  // For font scaling we need min/max passengers
  const passengerVals = busRows
    .map(r => parseFloat(r[KEY_AVG_PASSENGERS]))
    .filter(v => !Number.isNaN(v));
  const pMin = passengerVals.length ? Math.min(...passengerVals) : 0.1;
  const pMax = passengerVals.length ? Math.max(...passengerVals) : 66;
  const minFont = 10;
  const maxFont = 26;
  function scaleFont(p) {
    let v = parseFloat(p);
    if (Number.isNaN(v)) v = pMin;
    const t = (pMax === pMin) ? 0.5 : (v - pMin) / (pMax - pMin);
    const curved = Math.sqrt(Math.max(0, Math.min(1, t)));
    return minFont + curved * (maxFont - minFont);
  }

  const busItems = [];
  busRows.forEach(row => {
    const busTimeVal = getBusTimeByDirection(row);
    const y = timeToY(busTimeVal);
    if (y === null) return;
    const shilot = getSignageValue(row);
    const x = shilotToX.get(shilot);
    if (x === undefined) return;
    const trainId = getTrainIdValue(row);
    if (!trainId) return;
    const avgPassengers = row[KEY_AVG_PASSENGERS];
    const fontSize = (scaleFont(avgPassengers) * 3);

    busItems.push({
      row,
      x,
      y,
      trainId,
      fontSize,
      makat: getMakatValue(row),
      timeStr: extractHHMM(busTimeVal) || String(busTimeVal)
    });
  });

  // Draw each bus as circle at (x=signage column, y=timeToY)
  busItems.forEach(item => {
    const { row, x, y, trainId, fontSize, makat, timeStr } = item;
    const dotX = x;
    const dotRadius = Math.max(0.5, fontSize * 0.05);
    const busKey = buildBusRowKey(row);

    const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    dot.setAttribute("cx", dotX);
    dot.setAttribute("cy", y);
    dot.setAttribute("r", String(dotRadius));

    // green if on time, red if not
    const onTime = Number(row[KEY_IS_BUS_ON_TIME]);
    const busColor = (onTime === 1) ? "#13ba34" : "#ed0c0c";
    dot.setAttribute("fill", busColor);

    // building the bus dots:
    dot.classList.add("bus-item");
    const connectedTrainUid = getConnectedTrainUidForBusRow(row);
    dot.setAttribute("data-train-uid", connectedTrainUid);
    dot.setAttribute("data-signage", getSignageValue(row));
    dot.setAttribute("data-bus-key", busKey);
    dot.setAttribute("data-origin-cx", String(dotX));
    dot.setAttribute("data-origin-cy", String(y));

    // Click => show only this row in the right-side table + select the train + activate override + activate pop up
    dot.addEventListener("click", (e) => {
        e.stopPropagation();

        // keep only clicked dot
        setActiveBusRow(row);

        // train visuals only (opacity, perc, emoji)
        const trainUid = getConnectedTrainUidForBusRow(row);
        setActiveTrain(trainUid);

        //simulation row activation
        simulationSourceBusRow = row;

        // popup
        const popup = document.querySelector(".manual-sync-popup");
        const popupBtn = document.querySelector(".manual-sync-popup-btn");
        if (popup && !manualSyncPromptUsed) popup.classList.add("open");

        const historyPopup = document.querySelector(".manual-sync-history");
        if (historyPopup && !manualSyncPromptUsed) historyPopup.classList.add("open");

        const simulationPopup = document.querySelector(".manual-sync-simulation");
        if (simulationPopup && !manualSyncPromptUsed) simulationPopup.classList.add("open");

        if (popupBtn) {
            popupBtn.onclick = (ev) => {
            ev.stopPropagation();

            linkModeSourceRow = row;
            linkModeSourceBusKey = busKey;
            linkModeSourceDot = dot;
            isLinkDragging = false;
            linkModeConnectedTrainUid = getConnectedTrainUidForBusRow(row);
            applyLinkModeVisuals();
            manualSyncPromptUsed = true;
            closeManualSyncPopups();
            const confirmPopup = document.querySelector(".manual-sync-confirm");
            if (confirmPopup) {
                confirmPopup.classList.add("open");
                setTimeout(() => confirmPopup.classList.remove("open"), 3000);
                }
            };
        }
        const simulationBtn = document.querySelector(".manual-sync-simulation-btn");
        if (simulationBtn) {
          simulationBtn.onclick = (ev) => {
            ev.stopPropagation();
            simulationCaptureActive = true;
            renderSimulationBusTable(); // builds header/body + shows wrap

            const simIds = {
              top: "simulationTableTopScroll",
              topInner: "simulationTableTopScrollInner",
              main: "simulationTableMainScroll",
              table: "simulationTable",
            };

            bindTableScrollSync(simIds);
            updateTopTableScrollbar(simIds);
            alignTableScrollToRight(simIds);

            closeManualSyncPopups();
          };
        }
    });




    // drag mode for a bus dot
    dot.addEventListener("pointerdown", (e) => {
      if (!linkModeSourceDot || linkModeSourceDot !== dot) return;
      if (e.button !== 0) return;
      e.preventDefault();
      e.stopPropagation();
      isLinkDragging = true;
      dot.setPointerCapture(e.pointerId);
    });

    // hoovering for bus label
    const bTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
    const avgPassengersText = (row[KEY_AVG_PASSENGERS] === null || row[KEY_AVG_PASSENGERS] === undefined || String(row[KEY_AVG_PASSENGERS]).trim() === "")
      ? "—"
      : String(row[KEY_AVG_PASSENGERS]).trim();
    bTitle.textContent =
      "זמן: " + timeStr +
      " | מק\"ט: " + (makat || "—") +
      " | ממוצע נוסעים לנסיעה: " + avgPassengersText;

    dot.appendChild(bTitle);
    svg.appendChild(dot);

    // creates the "!' is there is a large SD:
    if (hasLargeArrivalWindow(row)) {
      const alert = document.createElementNS("http://www.w3.org/2000/svg", "text");
      alert.textContent = "!";
      alert.setAttribute("x", String(dotX + dotRadius + 6));
      alert.setAttribute("y", String(y));
      alert.setAttribute("font-size", "12");
      alert.setAttribute("font-weight", "700");
      alert.setAttribute("fill", "#000");
      alert.setAttribute("text-anchor", "start");
      alert.setAttribute("dominant-baseline", "middle");
      alert.classList.add("bus-item");
      alert.classList.add("bus-alert");
      alert.setAttribute("data-train", trainId);
      alert.setAttribute("data-signage", getSignageValue(row));
      alert.setAttribute("data-bus-key", busKey);

      const aTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
      aTitle.textContent = "סטיית תקן משעת ההגעה לתחנה: " + String(getArrivalWindowInt(row) ?? "") + " דק";
      alert.appendChild(aTitle);
      svg.appendChild(alert);
    }
  });

  // Ensure bus visibility + perc visibility matches selected train state
  applyBusFilter();
  applyPercFilter();
  applyLinkModeVisuals();
}

// endregion Map - Bus


// region manualy sync
let linkModeSourceBusKey = null;
let linkModeSourceRow = null;
let linkModeSourceDot = null;
let linkModeConnectedTrainUid = null;

let isLinkDragging = false;
let manualSyncPromptUsed = false;

const KEY_INTERNAL_TRAIN_STATION_CODE = "__train_station_code";
const KEY_INTERNAL_FROM_TRAIN_NUMBER = "__from_train_number";
const KEY_INTERNAL_FROM_TRAIN_ARRIVAL = "__from_train_rishui_train_arrival_time";


function applyLinkModeVisuals() {
    const busEls = svg.querySelectorAll(".bus-item");
    if (!linkModeSourceBusKey) {
        busEls.forEach(el => { el.style.display = ""; });
        return;
    }

    busEls.forEach(el => {
        const k = String(el.getAttribute("data-bus-key") || "");
        el.style.display = (k && k === linkModeSourceBusKey) ? "" : "none";
    });

    clearTrainOutlines();
    if (linkModeConnectedTrainUid) {
        const tri = svg.querySelector('.train-item[data-train-uid="' + linkModeConnectedTrainUid + '"]');
        if (tri) {
            tri.setAttribute("opacity", "1");
        }
    }
}

function exitLinkMode() {
    linkModeSourceRow = null;
    linkModeSourceBusKey = null;
    linkModeSourceDot = null;
    isLinkDragging = false;
    linkModeConnectedTrainUid = null;
    applyLinkModeVisuals();
    manualSyncPromptUsed = false;
}

async function saveDraggedOverride(sourceRow, targetRow) {
    if (!sourceRow || !targetRow) return;
    if (!Number.isFinite(activeYear) || !Number.isFinite(activeMonth)) return;

    let changeReason = "";
    while (!changeReason) {
        const input = window.prompt("יש להזין סיבת שינוי:", "");
        if (input === null) return; // cancel -> stop action
        changeReason = String(input).trim();
        if (!changeReason) window.alert("יש להזין סיבת שינוי");
    }

    const payload = {
        week_period: String(sourceRow[KEY_WEEK_PERIOD] ?? "").trim() || String(activeWeek ?? "").trim(),
        link_direction: (activeDirection === BUS_TO_RAIL_DIRECTION) ? "bus_to_rail" : "rail_to_bus",
        makat: sourceRow[KEY_MAKAT],
        direction: sourceRow[KEY_DIRECTION],
        alternative: String(sourceRow[KEY_ALTERNATIVE] ?? "").trim(),
        departure_time: String(sourceRow[KEY_BUS_DEPARTURE_TIME] ?? "").trim(),
        train_station_code: sourceRow[KEY_INTERNAL_TRAIN_STATION_CODE],
        from_train_number: sourceRow[KEY_INTERNAL_FROM_TRAIN_NUMBER],
        from_train_rishui_train_arrival_time: String(sourceRow[KEY_INTERNAL_FROM_TRAIN_ARRIVAL] ?? "").trim(),
        to_train_number: targetRow[KEY_TRAIN_ID],
        to_train_rishui_train_arrival_time: String(targetRow[KEY_TRAIN_ARRIVAL_RISHUI] ?? "").trim(),
        effective_month: String(activeYear) + "-" + String(activeMonth).padStart(2, "0"),
        change_reason: changeReason,
        changed_by: "",
    };

    const ok = window.confirm("מתבצעת התאמה לרכבת אחרת עבור אוטובוס זה , שינוי זה ישפיע על סנכרונים עתידיים, האם להמשיך?");
    if (!ok) {
        exitLinkMode();
        return;
    }

    try {
        await postOverride("/convergence/override/save/", payload);
        window.alert("החלפה נשמרה בהצלחה.");
        exitLinkMode();
        redrawBuses();
    } catch (e) {
        window.alert("שמירת ההחלפה נכשלה");
    }
}

function findRowByTrainUid(targetUid) {
    const rows = getFilteredTimes();
    for (const row of rows) {
      const trainId = getTrainIdValue(row);
      if (!trainId) continue;

      const normalizedTime = extractHHMM(row[KEY_TRAIN_ARRIVAL_RISHUI]);
      if (!normalizedTime) continue;

      const uid = trainId + "||" + normalizedTime;
      if (uid === targetUid) return row;
    }
    return null;
}

function getSvgPoint(clientX, clientY) {
    const pt = svg.createSVGPoint();
    pt.x = clientX;
    pt.y = clientY;
    return pt.matrixTransform(svg.getScreenCTM().inverse());
}

svg.addEventListener("pointermove", (e) => {
  if (!isLinkDragging || !linkModeSourceDot) return;
  const p = getSvgPoint(e.clientX, e.clientY);
  linkModeSourceDot.setAttribute("cx", String(p.x));
  linkModeSourceDot.setAttribute("cy", String(p.y));
});

svg.addEventListener("pointerup", async (e) => {
  if (!isLinkDragging || !linkModeSourceDot) return;
  isLinkDragging = false;

  const originCx = linkModeSourceDot.getAttribute("data-origin-cx");
  const originCy = linkModeSourceDot.getAttribute("data-origin-cy");
  if (originCx !== null) linkModeSourceDot.setAttribute("cx", originCx);
  if (originCy !== null) linkModeSourceDot.setAttribute("cy", originCy);

  const el = document.elementFromPoint(e.clientX, e.clientY);
  const tri = el && el.closest ? el.closest(".train-item") : null;
  if (!tri) return;

  const targetUid = String(tri.getAttribute("data-train-uid") || "");
  if (!targetUid) return;
  const targetRow = findRowByTrainUid(targetUid);
  if (!targetRow) return;

  await saveDraggedOverride(linkModeSourceRow, targetRow);
});

function getCsrfToken(name = "csrftoken") {
  const cookies = document.cookie ? document.cookie.split(";") : [];
  for (const c of cookies) {
    const part = c.trim();
    if (part.startsWith(name + "=")) return decodeURIComponent(part.slice(name.length + 1));
  }
  return "";
}

async function postOverride(url, payload) {
  const res = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCsrfToken(),
    },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.ok) {
    const errText = data.error ? String(data.error) : ("http_" + res.status);
    throw new Error(errText);
  }
  return data;
}
// endregion manualy sync


// region finalizing
refreshAllAfterFilterChange();
populateYearDropdown();
populateMonthDropdown();
updateTitleFromState();
setBackLinkWithCurrentFilters();


// endregion finalizing



