(function () {
  const dataElement = document.getElementById("progress-snapshots-data");
  const canvasElement = document.getElementById("progressChart");
  const reloadButton = document.getElementById("reloadGraph");

  if (reloadButton) {
    reloadButton.addEventListener("click", () => {
      window.location.reload();
    });
  }

  if (!dataElement || !canvasElement) {
    return;
  }

  let snapshots = [];
  try {
    const raw = dataElement.textContent ?? "[]";
    snapshots = JSON.parse(raw);
  } catch (error) {
    console.error("Failed to parse progress snapshot data", error);
    return;
  }

  if (!Array.isArray(snapshots) || snapshots.length === 0) {
    return;
  }

  const timeSeries = snapshots
    .map((entry) => ({
      x: new Date(entry.captured_at).getTime(),
      scraped: entry.scraped,
      playersTotal: entry.players_total,
    }))
    .filter((entry) => Number.isFinite(entry.x))
    .sort((a, b) => a.x - b.x);

  if (timeSeries.length === 0 || typeof Chart === "undefined") {
    return;
  }

  const scraped = timeSeries.map((entry) => ({ x: entry.x, y: entry.scraped }));
  const playersTotal = timeSeries.map((entry) => ({ x: entry.x, y: entry.playersTotal }));

  const formatDateTime = (timestamp) => {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) {
      return "";
    }
    return date.toLocaleString();
  };

  const baseLegendLabelGenerator =
    Chart.defaults?.plugins?.legend?.labels?.generateLabels;

  new Chart(canvasElement, {
    type: "line",
    data: {
      datasets: [
        {
          label: "Scraped",
          data: scraped,
          borderColor: "rgba(75, 192, 192, 1)",
          backgroundColor: "rgba(75, 192, 192, 0.1)",
          tension: 0.2,
        },
        {
          label: "Players Total",
          data: playersTotal,
          borderColor: "rgba(54, 162, 235, 1)",
          backgroundColor: "rgba(54, 162, 235, 0.1)",
          tension: 0.2,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      stacked: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            generateLabels(chartInstance) {
              if (typeof baseLegendLabelGenerator !== "function") {
                return Chart.defaults.plugins.legend.labels.generateLabels(
                  chartInstance,
                );
              }
              const labels = baseLegendLabelGenerator(chartInstance);
              return labels.map((label) => {
                const dataset = chartInstance.data.datasets?.[label.datasetIndex];
                if (dataset?.label) {
                  return {
                    ...label,
                    text: dataset.label,
                  };
                }
                return label;
              });
            },
          },
        },
        tooltip: {
          callbacks: {
            title(tooltipItems) {
              const timestamp = tooltipItems[0]?.parsed?.x;
              if (!Number.isFinite(timestamp)) {
                return "";
              }
              const date = new Date(timestamp);
              return date.toUTCString();
            },
          },
        },
      },
      scales: {
        x: {
          type: "linear",
          min: timeSeries[0]?.x,
          max: timeSeries[timeSeries.length - 1]?.x,
          title: {
            display: true,
            text: "Captured At (local time)",
          },
          ticks: {
            callback: (value) => formatDateTime(value),
            maxRotation: 0,
            autoSkip: true,
          },
        },
        y: {
          title: {
            display: true,
            text: "Count",
          },
        },
      },
    },
  });
})();
