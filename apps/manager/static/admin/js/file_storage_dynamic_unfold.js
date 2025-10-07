(function () {
  function byId(id) { return document.getElementById(id); }
  function show(el, v) { if (el) el.style.display = v ? "" : "none"; }

  function init() {
    const typeEl = byId("id_type");
    if (!typeEl) return;

    // секции, которые мы пометили классами в fieldsets
    const s3Section = document.querySelector(".fs-section.fs-s3");
    const yaSection = document.querySelector(".fs-section.fs-yadisk");

    function toggle() {
      const v = (typeEl.value || "").toLowerCase();
      const isYadisk = v === "yadisk";
      show(s3Section, !isYadisk);
      show(yaSection, isYadisk);
    }

    typeEl.addEventListener("change", toggle);
    toggle(); // первичная инициализация
  }

  // Unfold грузит страницу динамически — подождём DOM
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
