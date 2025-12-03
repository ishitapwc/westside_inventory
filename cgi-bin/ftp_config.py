FTP_CONFIGS = {
    "WESTSIDE_SAP": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/csv",
        "remote_archive": "/archive",
    },
    "WESTSIDE_WS": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/ws_sales_order",
        "remote_archive": "/ws_sales_order_archive",
    },
    "WESTSIDE_TUL": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/tul_sales_order",
        "remote_archive": "/tul_sales_order_archive",
    },
    "WESTSIDE_ETP": {
        "host": "ftp.sahilkumar.in",
        "user": "sushilpwc@sahilkumar.in",
        "pass": "SahilPahuja@29",

        # Remote folders
        "remote_incoming": "/etp_sales_order",
        "remote_archive": "/etp_sales_order_archive",
    },
    "GLOBAL": {
        # SAP folders
        "sap_local_read": "/var/www/html/python/westside_inventory/csv",
        "sap_local_archive": "/var/www/html/python/westside_inventory/archive",
        "sap_local_log": "/var/www/html/python/westside_inventory/logs",

        # WS folders
        "ws_local_read": "/var/www/html/python/westside_inventory/ws_sales_order",
        "ws_local_archive": "/var/www/html/python/westside_inventory/ws_sales_order_archive",
        "ws_local_log": "/var/www/html/python/westside_inventory/ws_sales_order_logs",

        # TUL folders
        "tul_local_read": "/var/www/html/python/westside_inventory/tul_sales_order",
        "tul_local_archive": "/var/www/html/python/westside_inventory/tul_sales_order_archive",
        "tul_local_log": "/var/www/html/python/westside_inventory/tul_sales_order_logs",

        # ETP folders
        "etp_local_read": "/var/www/html/python/westside_inventory/etp_sales_order",
        "etp_local_archive": "/var/www/html/python/westside_inventory/etp_sales_order_archive",
        "etp_local_log": "/var/www/html/python/westside_inventory/etp_sales_order_logs",
    }
}
