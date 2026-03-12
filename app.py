#!/usr/bin/env python3
from __future__ import annotations

from hr_app import create_app


app = create_app()


if __name__ == "__main__":
    app.run(debug=True, host="127.0.0.1", port=5000)


#data-visual.perenkap-api.online