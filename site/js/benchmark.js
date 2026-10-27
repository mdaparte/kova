(function () {
  var panel = document.createElement("div");
  panel.className = "bench-tip";
  panel.hidden = true;
  panel.setAttribute("role", "note");
  document.body.appendChild(panel);
  var open = null;

  function close() {
    panel.hidden = true;
    panel.classList.remove("is-op");
    if (open) open.setAttribute("aria-expanded", "false");
    open = null;
  }

  function place(button) {
    var limit = window.innerWidth - 12;
    var nav = document.querySelector(".onpage");
    if (nav && getComputedStyle(nav).display !== "none") {
      var navLeft = nav.getBoundingClientRect().left;
      if (navLeft > 80) limit = Math.min(limit, navLeft - 12);
    }
    panel.style.maxWidth = Math.max(180, limit - 8) + "px";
    var rect = button.getBoundingClientRect();
    var width = panel.offsetWidth;
    var height = panel.offsetHeight;
    var left = Math.min(rect.left, limit - width);
    var top = rect.bottom + 6;
    if (left < 8) left = 8;
    if (top + height > window.innerHeight - 8) top = Math.max(8, rect.top - height - 6);
    panel.style.left = left + "px";
    panel.style.top = top + "px";
  }

  function show(button, text, code) {
    panel.classList.toggle("is-op", code);
    panel.textContent = text;
    panel.hidden = false;
    place(button);
    button.setAttribute("aria-expanded", "true");
    open = button;
  }

  document.addEventListener("click", function (event) {
    var button = event.target.closest("button.ask, button.op");
    if (button) {
      event.preventDefault();
      if (open === button) {
        close();
        return;
      }
      close();
      if (button.classList.contains("op")) {
        show(button, button.getAttribute("data-op") || "", true);
      } else {
        var source = document.getElementById("tip-" + button.getAttribute("data-tip"));
        show(button, source ? source.textContent : "", false);
      }
      return;
    }
    if (!panel.hidden && !panel.contains(event.target)) close();
  });

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") close();
  });
  document.addEventListener("scroll", close, true);
})();
