#!/usr/bin/env python3
"""
Plotting script for BFT-Smart, PBFT, and Simplex benchmark results.
Visualizes latency comparison with confidence bands across runs.
"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def load_system_data(system_name, runs):
    """Load benchmark CSV files for a specific system."""
    data = {}
    csv_dir = Path("csv")
    
    for run in runs:
        filepath = csv_dir / f"{system_name}.R{run}.csv"
        if filepath.exists():
            data[f"R{run}"] = pd.read_csv(filepath)
            print(f"Loaded {filepath}")
        else:
            print(f"Warning: {filepath} not found")
    
    return data

def plot_latency_with_bands(data, title, filename):
    """Plot latency comparison with confidence bands."""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Get throughput points from first dataframe
    throughputs = list(data.values())[0]['Throughput'].values
    
    # Collect all latency values for each throughput level
    latency_matrix = []
    for label, df in data.items():
        latency_matrix.append(df['Latency (avg)'].values)
    
    latency_matrix = np.array(latency_matrix)
    
    # Calculate mean and std deviation
    mean_latency = np.mean(latency_matrix, axis=0)
    std_latency = np.std(latency_matrix, axis=0)
    
    # Plot individual runs as light lines
    colors = plt.cm.Set2(np.linspace(0, 1, len(data)))
    for (label, df), color in zip(data.items(), colors):
        ax.plot(df['Throughput'], df['Latency (avg)'], 
               marker='o', label=label, color=color, linewidth=1, alpha=0.4, markersize=4)
    
    # Plot mean with confidence band (±1 std dev)
    ax.plot(throughputs, mean_latency, color='black', linewidth=2.5, label='Mean', zorder=5)
    ax.fill_between(throughputs, 
                     mean_latency - std_latency, 
                     mean_latency + std_latency,
                     alpha=0.2, color='black', label='±1 Std Dev')
    
    # Add ±2 std dev band
    ax.fill_between(throughputs, 
                     mean_latency - 2*std_latency, 
                     mean_latency + 2*std_latency,
                     alpha=0.1, color='black', label='±2 Std Dev')
    
    ax.set_xlabel('Throughput (req/s)', fontsize=12)
    ax.set_ylabel('Latency (ms)', fontsize=12)
    ax.set_title(title, fontsize=13, fontweight='bold')
    ax.legend(loc='upper left', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    fig.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved: {filename}")
    return fig

def plot_combined_comparison(all_systems_data):
    """Plot all three systems on one graph."""
    fig, ax = plt.subplots(figsize=(13, 7))
    
    system_colors = {
        'BFT-Smart': '#1f77b4',
        'PBFT (New)': '#d62728',
        'Simplex': '#2ca02c'
    }
    
    for system_name, data in all_systems_data.items():
        # Get throughput and latency
        throughputs = list(data.values())[0]['Throughput'].values
        
        # Collect all latency values
        latency_matrix = []
        for label, df in data.items():
            latency_matrix.append(df['Latency (avg)'].values)
        
        latency_matrix = np.array(latency_matrix)
        
        # Calculate mean and std deviation
        mean_latency = np.mean(latency_matrix, axis=0)
        std_latency = np.std(latency_matrix, axis=0)
        
        color = system_colors[system_name]
        
        # Plot mean line
        ax.plot(throughputs, mean_latency, 
               color=color, linewidth=2.5, label=system_name, marker='o', markersize=5)
        
        # Plot confidence band
        ax.fill_between(throughputs, 
                        mean_latency - std_latency, 
                        mean_latency + std_latency,
                        alpha=0.15, color=color)
    
    ax.set_xlabel('Throughput (req/s)', fontsize=12)
    ax.set_ylabel('Latency (ms)', fontsize=12)
    ax.set_title('Latency Comparison: BFT-Smart vs PBFT (New) vs Simplex', fontsize=13, fontweight='bold')
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    fig.savefig('images/combined_comparison.png', dpi=300, bbox_inches='tight')
    print("Saved: images/combined_comparison.png")
    return fig

def plot_pbft_comparison(pbft_with_data, pbft_new_data):
    """Plot PBFT.With.Gorums vs PBFT.Gorums.New comparison."""
    fig, ax = plt.subplots(figsize=(13, 7))
    
    system_colors = {
        'PBFT (Old)': '#ff7f0e',
        'PBFT (New)': '#d62728'
    }
    
    systems_data = {
        'PBFT (Old)': pbft_with_data,
        'PBFT (New)': pbft_new_data
    }
    
    for system_name, data in systems_data.items():
        # Get throughput and latency
        throughputs = list(data.values())[0]['Throughput'].values
        
        # Collect all latency values
        latency_matrix = []
        for label, df in data.items():
            latency_matrix.append(df['Latency (avg)'].values)
        
        latency_matrix = np.array(latency_matrix)
        
        # Calculate mean and std deviation
        mean_latency = np.mean(latency_matrix, axis=0)
        std_latency = np.std(latency_matrix, axis=0)
        
        color = system_colors[system_name]
        
        # Plot mean line
        ax.plot(throughputs, mean_latency, 
               color=color, linewidth=2.5, label=system_name, marker='o', markersize=5)
        
        # Plot confidence band
        ax.fill_between(throughputs, 
                        mean_latency - std_latency, 
                        mean_latency + std_latency,
                        alpha=0.15, color=color)
    
    ax.set_xlabel('Throughput (req/s)', fontsize=12)
    ax.set_ylabel('Latency (ms)', fontsize=12)
    ax.set_title('PBFT Comparison: Old vs New Implementation', fontsize=13, fontweight='bold')
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    fig.savefig('images/pbft_comparison.png', dpi=300, bbox_inches='tight')
    print("Saved: images/pbft_comparison.png")
    return fig

def main():
    """Main function to generate plots."""
    # Create images folder if it doesn't exist
    images_dir = Path("images")
    images_dir.mkdir(exist_ok=True)
    
    print("\n=== Loading Benchmark Data ===\n")
    
    # Load data for all three systems
    bft_smart_data = load_system_data("BFT.Smart.Gorums", runs=[0, 1, 2, 3, 4])
    pbft_data = load_system_data("PBFT.Gorums.New", runs=[0, 1, 2, 3, 4])
    simplex_data = load_system_data("Simplex.Gorums", runs=[0, 1, 2, 3, 4, 5])
    pbft_with_data = load_system_data("PBFT.With.Gorums", runs=[0, 1, 2, 3, 4])
    
    if not bft_smart_data or not pbft_data or not simplex_data or not pbft_with_data:
        print("Failed to load some data. Exiting.")
        return
    
    print("\n=== Generating Individual Plots ===\n")
    
    # Generate individual plots for each system
    plot_latency_with_bands(
        bft_smart_data, 
        'BFT-Smart: Latency with Confidence Bands',
        'images/bft_smart_latency.png'
    )
    
    plot_latency_with_bands(
        pbft_data,
        'PBFT.Gorums.New (New): Latency with Confidence Bands',
        'images/pbft_latency.png'
    )
    
    plot_latency_with_bands(
        pbft_with_data,
        'PBFT.With.Gorums (Old): Latency with Confidence Bands',
        'images/pbft_with_latency.png'
    )
    
    plot_latency_with_bands(
        simplex_data,
        'Simplex: Latency with Confidence Bands',
        'images/simplex_latency.png'
    )
    
    print("\n=== Generating Combined Comparisons ===\n")
    
    # Generate combined comparison plot for three main systems
    all_systems = {
        'BFT-Smart': bft_smart_data,
        'PBFT (New)': pbft_data,
        'Simplex': simplex_data
    }
    plot_combined_comparison(all_systems)
    
    # Generate PBFT-specific comparison
    plot_pbft_comparison(pbft_with_data, pbft_data)
    
    print("\n=== Complete ===")
    plt.show()

if __name__ == "__main__":
    main()
