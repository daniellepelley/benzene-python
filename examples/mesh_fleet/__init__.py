"""A three-service Benzene mesh, in-process, that populates the mesh-UI catalog artifacts.

Modelled on the .NET ``examples/Mesh`` (orders / payments / shipping): each service self-describes
(``/benzene/spec``), reports health (``/benzene/health``), and registers + heartbeats + traces to a
shared :class:`~benzene.mesh.MeshCollector`. ``orders`` calls ``payments`` and ``shipping`` — forwarding
its mesh span — so the collector derives the ``orders → payments`` / ``orders → shipping`` /
``payments → shipping`` consumer edges from trace parentage. :func:`fleet.build_fleet` runs the traffic
and returns the catalog spine + collector; :mod:`prove` feeds them to the emitter and renders the result
in the canonical mesh UI via headless Chromium.
"""
