"""Python presentation layer; all Monte Carlo computations run in Scala Spark."""
import csv
import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

ROOT = Path(__file__).resolve().parent


class PiLab:
    def __init__(self, window):
        self.window = window
        self.events = queue.Queue()
        self.results = []
        window.title("π Lab — Scala Spark + Python UI")
        window.geometry("1050x780")
        frame = ttk.Frame(window, padding=20)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text="Estimating π with Spark RDDs", font=("Segoe UI", 22, "bold")).pack(anchor="w")
        ttk.Label(frame, text="Scala computes • Python displays | π ≈ 4 × inside / total").pack(anchor="w", pady=(4, 15))
        controls = ttk.Frame(frame)
        controls.pack(fill="x")
        self.samples = tk.StringVar(value="1000000")
        self.partitions = tk.StringVar(value="4")
        self.method = tk.StringVar(value="count")
        for title, variable, choices in [
            ("Samples", self.samples, ["1000", "100000", "1000000", "10000000", "100000000"]),
            ("Partitions", self.partitions, ["1", "2", "4", "8"]),
            ("Method", self.method, ["count", "reduce"]),
        ]:
            ttk.Label(controls, text=title).pack(side="left", padx=(0, 5))
            ttk.Combobox(controls, textvariable=variable, values=choices, width=14,
                         state="readonly" if title == "Method" else "normal").pack(side="left", padx=(0, 15))
        self.run_button = ttk.Button(controls, text="Run Spark", command=self.run)
        self.run_button.pack(side="left")
        ttk.Button(controls, text="Export CSV", command=self.export).pack(side="left", padx=8)
        self.status = tk.StringVar(value="Ready. First build: powershell -File build.ps1")
        ttk.Label(frame, textvariable=self.status, wraplength=980).pack(anchor="w", pady=12)
        self.metrics = tk.StringVar(value="π estimate: —    Absolute error: —    Spark compute time: —")
        ttk.Label(frame, textvariable=self.metrics, font=("Segoe UI", 14, "bold")).pack(anchor="w")
        self.canvas = tk.Canvas(frame, height=330, background="#f4f7fc", highlightthickness=0)
        self.canvas.pack(fill="x", pady=12)
        self.draw([])
        columns = ("samples", "partitions", "method", "inside", "estimate", "error", "seconds")
        self.table = ttk.Treeview(frame, columns=columns, show="headings", height=6)
        for column in columns:
            self.table.heading(column, text=column.title())
            self.table.column(column, width=125, anchor="center")
        self.table.pack(fill="both", expand=True)
        self.log = tk.Text(frame, height=4, wrap="word", font=("Consolas", 9))
        self.log.pack(fill="x", pady=(10, 0))
        window.after(100, self.poll)

    def draw(self, points):
        self.canvas.delete("all")
        self.canvas.create_rectangle(25, 15, 325, 315, outline="#64748b", width=2)
        self.canvas.create_oval(25, 15, 325, 315, outline="#334155", width=2)
        for x, y in points:
            px, py = 175 + x * 150, 165 - y * 150
            color = "#10a37f" if x*x + y*y <= 1 else "#f97316"
            self.canvas.create_oval(px-2, py-2, px+2, py+2, fill=color, outline="")
        for index, text in enumerate([
            "Green: inside the circle | Orange: outside",
            "Inside when x² + y² ≤ 1",
            "Square area = 4 | Circle area = π",
            "π ≈ 4 × (inside points / total points)",
            "Preview: up to 800 separate Scala-generated points.",
            "The estimate uses ALL requested samples.",
        ]):
            self.canvas.create_text(355, 50 + index*38, text=text, anchor="w", font=("Segoe UI", 11))

    def run(self):
        try:
            n, partitions = int(self.samples.get()), int(self.partitions.get())
            if not 1 <= n <= 100000000 or not 1 <= partitions <= 128:
                raise ValueError("Samples: 1–100000000. Partitions: 1–128.")
        except ValueError as error:
            messagebox.showerror("Invalid settings", str(error))
            return
        jar = ROOT / "build" / "pi-estimator.jar"
        submit = shutil.which("spark-submit.cmd") or shutil.which("spark-submit")
        if not jar.exists() or not submit:
            messagebox.showerror("Setup needed", "Build the JAR with build.ps1 and add Spark bin to PATH.")
            return
        self.run_button.configure(state="disabled")
        self.status.set("Spark is running… startup may take a few seconds. UI remains responsive.")
        self.log.delete("1.0", "end")
        method = self.method.get()
        threading.Thread(target=self.worker, args=(submit, jar, n, partitions, method), daemon=True).start()

    def worker(self, submit, jar, n, partitions, method):
        command = [submit, "--master", "local[*]", "--conf", "spark.ui.enabled=true",
                   "--class", "PiEstimator", str(jar), str(n), str(partitions), method]
        if os.name == "nt":
            command = [os.environ.get("COMSPEC", "cmd.exe"), "/c", *command]
        try:
            result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8",
                                    errors="replace", cwd=ROOT,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            if result.returncode:
                raise RuntimeError((result.stderr + result.stdout)[-10000:])
            lines = [line for line in result.stdout.splitlines() if line.startswith("PI_RESULT=")]
            if not lines:
                raise RuntimeError("Spark returned no result.\n" + result.stderr[-5000:])
            self.events.put(("success", json.loads(lines[-1].split("=", 1)[1])))
        except Exception as error:
            self.events.put(("error", str(error)))

    def poll(self):
        try:
            kind, value = self.events.get_nowait()
            self.run_button.configure(state="normal")
            if kind == "success":
                self.results.append(value)
                self.metrics.set(f"π estimate: {value['estimate']:.6f}    Absolute error: {value['error']:.6f}    Spark compute time: {value['seconds']:.3f} s")
                self.status.set("Done. Compute time excludes JVM/Spark startup and the diagram sample. Spark UI closes when the job ends.")
                self.draw(value["preview"])
                self.table.insert("", "end", values=(value["samples"], value["partitions"], value["method"], value["inside"], f"{value['estimate']:.6f}", f"{value['error']:.6f}", f"{value['seconds']:.3f}"))
            else:
                self.status.set("Spark failed. See details below.")
                self.log.insert("end", value)
        except queue.Empty:
            pass
        self.window.after(100, self.poll)

    def export(self):
        if not self.results:
            messagebox.showinfo("Export", "Run an experiment first.")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", initialfile="pi-results.csv")
        if path:
            columns = ["samples", "partitions", "method", "inside", "estimate", "error", "seconds"]
            with open(path, "w", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(self.results)


if __name__ == "__main__":
    window = tk.Tk()
    PiLab(window)
    window.mainloop()
