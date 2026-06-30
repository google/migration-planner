import flet as ft
import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

class CustomLineChart(ft.Container):
    def __init__(self, dates, datasets, colors, height=300):
        super().__init__()
        self.dates = dates
        self.datasets = datasets
        self.colors = colors
        self.chart_height = height
        
        self.content = self.create_chart_image()
        self.height = self.chart_height
        self.expand = True

    def create_chart_image(self):
        if not self.dates or not self.datasets:
            return ft.Container()
            
        fig, ax = plt.subplots(figsize=(10, 4))

        for name, data in self.datasets.items():
            color = self.colors.get(name, "#000000")
            ax.plot(self.dates, data, label=name, color=color, marker='o', markersize=5, linewidth=2.5)

        # Formatting
        fig.patch.set_facecolor('none')
        ax.set_facecolor('none')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#E2E8F0')
        ax.spines['bottom'].set_color('#E2E8F0')
        ax.grid(axis='y', color='#E2E8F0', linestyle='-', linewidth=1)
        ax.tick_params(axis='both', colors='#64748B', labelsize=10)
        
        # Adjust x ticks to not overlap
        num_dates = len(self.dates)
        label_interval = max(1, num_dates // 6)
        ax.set_xticks(range(0, num_dates, label_interval))
        ax.set_xticklabels([self.dates[i] for i in range(0, num_dates, label_interval)])

        # Apply tight layout to minimize whitespace
        fig.tight_layout()

        # Save to buffer
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight', transparent=True, dpi=120)
        buf.seek(0)
        
        # Close fig
        plt.close(fig)
        
        b64_string = base64.b64encode(buf.read()).decode('utf-8')
        
        return ft.Image(src=f"data:image/png;base64,{b64_string}", fit="contain", expand=True)
