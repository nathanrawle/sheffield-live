(() => {
  const timeline = document.querySelector("[data-venue-timeline]");
  if (!timeline) {
    return;
  }

  const narrowQuery = window.matchMedia("(max-width: 700px)");
  let fullRenderScheduled = false;
  let mobileRenderScheduled = false;

  function venueDayGroups() {
    const groups = new Map();
    timeline.querySelectorAll("[data-venue-day]").forEach((item) => {
      const key = item.dataset.venueDay;
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(item);
    });
    return Array.from(groups.values());
  }

  function rowsForItems(items) {
    return items.reduce((rows, item) => {
      const rect = item.getBoundingClientRect();
      const row = rows.find((candidate) => Math.abs(candidate.top - rect.top) < 4);
      if (row) {
        row.items.push(item);
        return rows;
      }
      rows.push({ top: rect.top, items: [item] });
      return rows;
    }, []).map((row) => ({
      ...row,
      items: row.items.sort((left, right) => left.getBoundingClientRect().left - right.getBoundingClientRect().left),
    }));
  }

  function appendChevron(direction, color, left, top) {
    const chevron = document.createElement("span");
    chevron.className = `venue-day-chevron venue-day-chevron-${direction}`;
    chevron.setAttribute("aria-hidden", "true");
    chevron.style.setProperty("--venue-day-chevron-color", color);
    chevron.style.left = `${left}px`;
    chevron.style.top = `${top}px`;
    timeline.append(chevron);
  }

  function renderChevrons() {
    timeline.querySelectorAll(".venue-day-chevron").forEach((chevron) => chevron.remove());
    if (narrowQuery.matches) {
      return;
    }

    const timelineRect = timeline.getBoundingClientRect();
    const chevronWidth = 15;

    venueDayGroups().forEach((items) => {
      const rows = rowsForItems(items);
      if (rows.length < 2) {
        return;
      }

      const styles = getComputedStyle(items[0]);
      const color = styles.getPropertyValue("--venue-day-accent-muted").trim() || styles.getPropertyValue("--venue-day-accent").trim();

      rows.forEach((row, rowIndex) => {
        const firstItem = row.items[0];
        const lastItem = row.items[row.items.length - 1];
        const marker = firstItem.querySelector(".venue-day-pill");
        const firstCard = firstItem.querySelector(".event-card");
        const lastCard = lastItem.querySelector(".event-card");
        if (!marker || !firstCard || !lastCard) {
          return;
        }

        const markerRect = marker.getBoundingClientRect();
        const firstItemRect = firstItem.getBoundingClientRect();
        const firstCardRect = firstCard.getBoundingClientRect();
        const lastCardRect = lastCard.getBoundingClientRect();
        const isFirstRow = rowIndex === 0;
        const isLastRow = rowIndex === rows.length - 1;
        const top = markerRect.top + markerRect.height / 2 - timelineRect.top - 7;

        if (!isFirstRow) {
          const gutterStart = firstItemRect.left - timelineRect.left;
          const gutterEnd = firstCardRect.left - timelineRect.left;
          const left = gutterStart + Math.max(0, gutterEnd - gutterStart - chevronWidth) / 2;
          appendChevron("in", color, left, top);
        }

        if (!isLastRow) {
          appendChevron("out", color, lastCardRect.right - timelineRect.left + 4, top);
        }
      });
    });
  }

  function mobileMarker() {
    let marker = document.querySelector(".venue-mobile-day-marker");
    if (marker) {
      return marker;
    }

    marker = document.createElement("div");
    marker.className = "venue-mobile-day-marker";
    marker.setAttribute("aria-hidden", "true");
    marker.innerHTML = `
      <span class="venue-day-pill">
        <span class="venue-day-title"></span>
        <span class="venue-day-meta"></span>
      </span>`;
    document.body.append(marker);
    return marker;
  }

  function renderMobileMarker() {
    const marker = mobileMarker();
    if (!narrowQuery.matches) {
      marker.classList.remove("is-visible");
      return;
    }

    const headerBottom = document.querySelector(".topbar")?.getBoundingClientRect().bottom || 0;
    const activeY = headerBottom + 16;
    const activeGroup = venueDayGroups().find((items) => {
      const firstRect = items[0].getBoundingClientRect();
      const lastRect = items[items.length - 1].getBoundingClientRect();
      return firstRect.top <= activeY && lastRect.bottom > activeY;
    });

    if (!activeGroup) {
      marker.classList.remove("is-visible");
      return;
    }

    const source = activeGroup[0];
    const title = source.querySelector(".venue-day-title")?.textContent || "";
    const meta = source.querySelector(".venue-day-meta")?.textContent || "";
    const accent = getComputedStyle(source).getPropertyValue("--venue-day-accent").trim();

    marker.querySelector(".venue-day-title").textContent = title;
    marker.querySelector(".venue-day-meta").textContent = meta;
    marker.style.setProperty("--venue-mobile-day-accent", accent);
    marker.classList.add("is-visible");
  }

  function render() {
    renderChevrons();
    renderMobileMarker();
  }

  function scheduleRender() {
    if (fullRenderScheduled) {
      return;
    }
    fullRenderScheduled = true;
    window.requestAnimationFrame(() => {
      fullRenderScheduled = false;
      render();
    });
  }

  function scheduleMobileMarkerRender() {
    if (mobileRenderScheduled) {
      return;
    }
    mobileRenderScheduled = true;
    window.requestAnimationFrame(() => {
      mobileRenderScheduled = false;
      renderMobileMarker();
    });
  }

  render();
  window.addEventListener("resize", scheduleRender);
  window.addEventListener("scroll", scheduleMobileMarkerRender, { passive: true });
  if (narrowQuery.addEventListener) {
    narrowQuery.addEventListener("change", scheduleRender);
  } else {
    narrowQuery.addListener(scheduleRender);
  }
  if (document.fonts) {
    document.fonts.ready.then(scheduleRender);
  }
})();
