#!/usr/bin/env python3
"""
Simple HTTP server to serve the equity dashboard locally.
Run this script and open http://localhost:8000 to view the dashboard.
"""
import http.server
import socketserver
import webbrowser
import os
from pathlib import Path

def serve_dashboard(port=8000):
    """Serve the dashboard on the specified port."""
    # Change to the html directory
    html_dir = Path(__file__).parent
    os.chdir(html_dir)
    
    print(f"Starting dashboard server...")
    print(f"Directory: {html_dir}")
    print(f"Port: {port}")
    
    # Create server
    handler = http.server.SimpleHTTPRequestHandler
    
    # Add CORS headers for local development
    class CORSHTTPRequestHandler(handler):
        def end_headers(self):
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
            self.send_header('Access-Control-Allow-Headers', 'Content-Type')
            super().end_headers()
    
    with socketserver.TCPServer(("", port), CORSHTTPRequestHandler) as httpd:
        print(f"\n🌐 Dashboard available at: http://localhost:{port}/equity_dashboard.html")
        print(f"📊 Direct link: http://localhost:{port}/equity_dashboard.html")
        print("\nPress Ctrl+C to stop the server")
        
        # Try to open browser automatically
        try:
            webbrowser.open(f"http://localhost:{port}/equity_dashboard.html")
            print("✨ Opening dashboard in your default browser...")
        except Exception as e:
            print(f"Could not auto-open browser: {e}")
        
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\n👋 Dashboard server stopped")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Serve the Equity ETL Dashboard")
    parser.add_argument("--port", type=int, default=8000, 
                       help="Port to serve on (default: 8000)")
    
    args = parser.parse_args()
    serve_dashboard(args.port)