// Every page says that it describes `main`, which may be ahead of the latest
// release.
(function () {
    const main = document.querySelector("main");
    if (!main) return;
    const banner = document.createElement("div");
    banner.className = "gnitz-banner";
    banner.innerHTML =
        'These docs describe gnitz as it is on <code>main</code>, which may be ahead of the ' +
        '<a href="https://github.com/Ed-von-Schleck/gnitz/releases/latest">latest release</a>.';
    main.prepend(banner);
})();
