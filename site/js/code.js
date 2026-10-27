(function () {
  var langs = {
    sh: {
      keywords: ["curl", "sh", "docker", "mkdir", "cd", "tar", "export", "mongosh", "kovadump", "cat"]
    },
    mql: {
      keywords: ["db", "true", "false", "null", "NumberInt", "ObjectId", "ISODate"]
    },
    sql: {
      keywords: ["SELECT", "FROM", "WHERE", "TEXT", "FUZZY", "FACETS", "TERMS", "RANGES", "AND", "OR", "GROUP", "BY", "ORDER", "AS", "COUNT", "SUM", "SHOW", "INDEXES", "INDEX", "KEYS", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE"]
    }
  };

  function esc(text) {
    return text
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function paint(code, lang) {
    var spec = langs[lang];
    if (!spec) return;
    var source = code.textContent;
    var html = "";
    var i = 0;
    while (i < source.length) {
      var rest = source.slice(i);
      var string = rest.match(/^(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')/);
      if (string) {
        html += '<span class="tok-s">' + esc(string[0]) + "</span>";
        i += string[0].length;
        continue;
      }
      var comment = rest.match(/^#[^\n]*/);
      if (comment && lang !== "mql") {
        html += '<span class="tok-c">' + esc(comment[0]) + "</span>";
        i += comment[0].length;
        continue;
      }
      var word = rest.match(/^(?:\$[A-Za-z_][\w.]*|[A-Za-z_][\w]*)/);
      if (word) {
        var token = word[0];
        var bare = token.replace(/^\$/, "");
        var key = spec.keywords.indexOf(token) !== -1 || spec.keywords.indexOf(token.toUpperCase()) !== -1 || spec.keywords.indexOf(bare) !== -1 || token.charAt(0) === "$";
        html += key ? '<span class="tok-k">' + esc(token) + "</span>" : esc(token);
        i += token.length;
        continue;
      }
      var number = rest.match(/^-?\d+(?:\.\d+)?/);
      if (number) {
        html += '<span class="tok-n">' + esc(number[0]) + "</span>";
        i += number[0].length;
        continue;
      }
      var flag = rest.match(/^--?[A-Za-z0-9-]+/);
      if (flag && lang === "sh") {
        html += '<span class="tok-f">' + esc(flag[0]) + "</span>";
        i += flag[0].length;
        continue;
      }
      html += esc(source.charAt(i));
      i += 1;
    }
    code.innerHTML = html;
  }

  document.querySelectorAll("pre code[class^='lang-']").forEach(function (code) {
    var lang = code.className.replace("lang-", "");
    if (lang === "out" || lang === "cfg") return;
    paint(code, lang);
  });

  var stored = null;
  try { stored = localStorage.getItem("kova-lang"); } catch (err) { stored = null; }

  function showLang(ex, lang) {
    ex.querySelectorAll(".ex-pane").forEach(function (pane) {
      pane.hidden = pane.getAttribute("data-lang") !== lang;
    });
    ex.querySelectorAll(".ex-pick").forEach(function (button) {
      button.setAttribute("aria-pressed", button.getAttribute("data-lang") === lang ? "true" : "false");
    });
  }

  function applyLang(lang) {
    document.querySelectorAll("[data-switch]").forEach(function (ex) {
      showLang(ex, lang);
    });
  }

  if (stored === "mql" || stored === "sql") applyLang(stored);

  document.querySelectorAll(".ex-pick").forEach(function (button) {
    button.addEventListener("click", function () {
      var lang = button.getAttribute("data-lang");
      try { localStorage.setItem("kova-lang", lang); } catch (err) { /* keep the click local */ }
      applyLang(lang);
    });
  });

  document.querySelectorAll(".ex").forEach(function (ex) {
    var header = ex.querySelector(":scope > .ex-label");
    if (!header || header.querySelector(".ex-copy")) return;
    var button = document.createElement("button");
    button.type = "button";
    button.className = "ex-copy";
    button.textContent = "Copy";
    button.setAttribute("aria-label", "Copy command");
    header.appendChild(button);
    button.addEventListener("click", function () {
      var scope = ex.querySelector(".ex-pane:not([hidden])") || ex;
      var code = scope.querySelector("pre code:not(.lang-out)");
      if (!code) return;
      var text = code.textContent;
      var done = function () {
        button.textContent = "Copied";
        window.setTimeout(function () { button.textContent = "Copy"; }, 1200);
      };
      var fallback = function () {
        var area = document.createElement("textarea");
        area.value = text;
        area.setAttribute("readonly", "");
        area.style.position = "fixed";
        area.style.left = "-9999px";
        document.body.appendChild(area);
        area.select();
        document.execCommand("copy");
        area.remove();
        done();
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(text).then(done).catch(fallback);
        return;
      }
      fallback();
    });
  });
})();
