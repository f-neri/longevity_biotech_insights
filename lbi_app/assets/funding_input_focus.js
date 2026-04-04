(function () {
    function isFundingInput(el) {
        return !!(el && el.classList && el.classList.contains("funding-number-input"));
    }

    function selectAll(el) {
        try {
            el.select();
        } catch (e) {
            // No-op for browsers that restrict selection on numeric inputs.
        }
    }

    document.addEventListener("focusin", function (event) {
        var target = event.target;
        if (isFundingInput(target)) {
            selectAll(target);
        }
    });

    document.addEventListener("pointerup", function (event) {
        var target = event.target;
        if (isFundingInput(target) && document.activeElement === target) {
            selectAll(target);
        }
    });
})();
