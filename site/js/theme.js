(function () {
  var key = "kova-theme";
  var root = document.documentElement;

  function current() {
    return root.getAttribute("data-theme") === "light" ? "light" : "dark";
  }

  function paint(theme) {
    root.setAttribute("data-theme", theme);
    var button = document.querySelector("[data-theme-toggle]");
    if (!button) return;
    var next = theme === "light" ? "dark" : "light";
    button.textContent = next;
    button.setAttribute("aria-pressed", theme === "light" ? "true" : "false");
    button.setAttribute("aria-label", "Switch to " + next + " theme");
  }

  paint(current());

  document.addEventListener("click", function (event) {
    var button = event.target.closest("[data-theme-toggle]");
    if (!button) return;
    var next = current() === "light" ? "dark" : "light";
    try {
      localStorage.setItem(key, next);
    } catch (error) {}
    paint(next);
  });
})();
