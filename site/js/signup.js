// Paste the address of a form that accepts a field named "email".
// A free Formspree form looks like https://formspree.io/f/xxxxxxxx
// Until this is set, the button tells the visitor the list is closed.
const SIGNUP_URL = "https://formspree.io/f/mbgggvog";

document.querySelectorAll("form.list").forEach((form) => {
  const status = form.querySelector(".status");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    status.className = "status";
    if (!SIGNUP_URL) {
      status.textContent = "The list is closed right now.";
      status.classList.add("err");
      return;
    }
    const response = await fetch(SIGNUP_URL, {
      method: "POST",
      body: new FormData(form),
      headers: { Accept: "application/json" },
    });
    if (response.ok) {
      form.reset();
      status.textContent = "Saved. We'll write about the next release.";
      status.classList.add("ok");
      return;
    }
    status.textContent = "That address did not go through. Try it again.";
    status.classList.add("err");
  });
});
