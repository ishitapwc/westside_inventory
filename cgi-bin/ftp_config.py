FTP_CONFIGS = {
    "WESTSIDE_1": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/csv",
        "remote_archive": "/archive",
        "remote_logs": "/logs",

        # Local folders
        "local_read": "/var/www/html/python/westside_inventory/csv",
        "local_archive": "/var/www/html/python/westside_inventory/archive",
        "local_log": "/var/www/html/python/westside_inventory/logs",
    },

    "WESTSIDE_2": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/sales_order",
        "remote_archive": "/sales_order_archive",
        "remote_logs": "/sales_order_logs",

        # Local folders
        "local_read": "/var/www/html/python/westside_inventory/orders",
        "local_archive": "/var/www/html/python/westside_inventory/orders_archive",
        "local_log": "/var/www/html/python/westside_inventory/orders_logs",
    }
}
