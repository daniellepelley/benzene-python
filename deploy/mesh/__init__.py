"""The distributed Benzene Mesh Host deployment demo — the multi-process shape of ``mesh_fleet``.

Four separate ASGI apps (the Mesh Host + orders/payments/shipping) on real localhost sockets, with the
fleet reporting into the host's collector over genuine HTTP feed pushes and the host serving the mesh UI.
See :mod:`deploy.mesh.stack` for the orchestration and ``prove.py`` for the browser-driven proof.
"""
