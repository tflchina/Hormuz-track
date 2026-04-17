const manifestPath = "timelapses/manifest.json";
const updatedAtEl = document.getElementById("updatedAt");
const captionEl = document.getElementById("caption");
const imageEl = document.getElementById("timelapseImage");
const buttons = Array.from(document.querySelectorAll(".window-btn"));

let manifest = null;
let currentWindow = 12;

function setActiveButton(hours) {
  buttons.forEach((btn) => btn.classList.toggle("active", Number(btn.dataset.window) === hours));
}

function render(hours) {
  if (!manifest || !manifest.timelapses?.[hours]) {
    imageEl.removeAttribute("src");
    captionEl.textContent = `No timelapse available for ${hours} hours yet.`;
    return;
  }

  const relPath = manifest.timelapses[hours];
  imageEl.src = relPath;
  imageEl.alt = `Ship timelapse near Strait of Hormuz, ${hours} hour window`;
  captionEl.textContent = `Showing latest ${hours}h timelapse (${relPath}).`;
}

async function loadManifest() {
  try {
    const res = await fetch(`${manifestPath}?t=${Date.now()}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    manifest = await res.json();

    updatedAtEl.textContent = manifest.updated_at_utc || "unknown";
    render(currentWindow);
  } catch (error) {
    updatedAtEl.textContent = "unavailable";
    imageEl.removeAttribute("src");
    captionEl.textContent = `Failed to load ${manifestPath}: ${error.message}`;
  }
}

buttons.forEach((btn) => {
  btn.addEventListener("click", () => {
    currentWindow = Number(btn.dataset.window);
    setActiveButton(currentWindow);
    render(currentWindow);
  });
});

setActiveButton(currentWindow);
loadManifest();
