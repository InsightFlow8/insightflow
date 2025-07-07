import pandas as pd
from pathlib import Path

def generate_batches(source_csv, output_dir, sample_frac=0.05):
    df = pd.read_csv(source_csv)
    sampled = df.sample(frac=sample_frac, random_state=42)
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    output_path = Path(output_dir) / f"{date_str}_sample.csv"
    sampled.to_csv(output_path, index=False)
    print(f"Sampled saved to {output_path}")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Generate CSV batches")
    parser.add_argument('--source', required=True, help='Source CSV file')
    parser.add_argument('--output-dir', required=True, help='Output directory for samples')
    parser.add_argument('--frac', type=float, default=0.05, help='Sample fraction')
    args = parser.parse_args()
    generate_batches(args.source, args.output_dir, args.frac)
