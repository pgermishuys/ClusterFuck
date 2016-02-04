fromAll().whenAny(function (s, e) {
    if (e.eventType[0] !== '$')
        linkTo("non-system", e);

    if (s.events === undefined)
        return { events: 1 };

    return { events: s.events + 1 };
});