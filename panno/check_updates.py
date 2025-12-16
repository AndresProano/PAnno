import requests
import os
import zipfile
import pandas as pd
import io

url = "https://api.pharmgkb.org/v1/download/file/data/clinicalAnnotations.zip"
url_api = "https://api.pharmgkb.org/v1"

temp_dir = "./panno/data/temp/"
os.makedirs(temp_dir, exist_ok=True)

output_dir = "./panno/data/output_review/"


#Generar ClinAnn
def actualizar_clinaan(conn):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extract("clinical_annotations.tsv", temp_dir)
            z.extract("clinical_ann_alleles.tsv", temp_dir)
            #z.extract("clinical_ann_evidence.tsv", temp_dir)
        
        df_meta = pd.read_csv(f"{temp_dir}clinical_annotations.tsv", sep="\t")
        df_alleles = pd.read_csv(f"{temp_dir}clinical_ann_alleles.tsv", sep="\t")
        #df_evidence = pd.read_csv(f"{temp_dir}clinical_ann_evidence.tsv", sep="\t")

        merged = pd.merge(df_alleles, df_meta, on="Clinical Annotation ID", how="inner")

        def split_alleles(g):
            if pd.isna(g) or len(g) != 2: return g, None # Fallback simple
            return g[0], g[1]

        merged[['Allele1', 'Allele2']] = merged['Genotype/Allele'].apply(
            lambda x: pd.Series(split_alleles(x))
        )

        df_final = pd.DataFrame()
        df_final['CAID'] = merged['Clinical Annotation ID']
        df_final['Gene'] = merged['Gene']
        df_final['Variant'] = merged['Variant/Haplotypes']
        df_final['Allele1'] = merged['Allele1']
        df_final['Allele2'] = merged['Allele2']
        df_final['Annotation1'] = merged['Annotation Text']
        df_final['Function1'] = merged['Allele Function']
        df_final['Function2'] = ""
        df_final['Score1'] = merged['Score']
        df_final['Score2'] = ""
        df_final['CPICPhenotype'] = merged['Phenotype Category']
        df_final['PAnnoPhenotype'] = merged['Allele Function']
        df_final['Drug'] = merged['Drug(s)']
        df_final['Phenotypes'] = merged['Phenotype(s)']
        df_final['EvidenceLevel'] = merged['Level of Evidence']
        df_final['LevelOverride'] = merged['Level Override']
        df_final['LevelModifier'] = merged["Level Modifiers"]
        df_final['Score'] = merged['Score']
        df_final['EvidenceCount'] = merged["Evidence Count"]
        df_final['Specialty'] = merged["Specialty Population"]
        df_final['PhenotypeCategory'] = merged['Phenotype Category']
        df_final.fillna("", inplace=True)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading PharmGKB guidelines: {e}")

def actualizar_guidelines(conn):
    try:
        response = requests.get(f"{url_api}/data/guideline")
        response.raise_for_status()  # Raise an error for bad status codes

        guidelines = response.json().get('data', [])
    except Exception as e:
        print(f"Error API: {e}")
        return

    merge_rows = []
    
    # Procesamos las guías (limitado a CPIC/DPWG para ejemplo)
    for g in guidelines:
        if g.get('source') not in ['CPIC', 'DPWG', 'RNPGx']: continue
        
        # Descargar detalle de la guía
        r_det = requests.get(f"{url_api}/data/guideline/{g['id']}")
        if r_det.status_code != 200: continue
        
        data = r_det.json().get('data', {})
        
        # Extraer Gene y Drug
        gene = data.get('relatedGenes', [{}])[0].get('symbol', 'Unknown')
        drug = data.get('relatedChemicals', [{}])[0].get('name', 'Unknown')

        for rec in data.get('recommendation', []):
            merge_rows.append({
                'ID': g['id'],
                'Source': g.get('source'),
                'Gene': gene,
                'Drug': drug,
                'Phenotype': rec.get('phenotype', ''),
                'Recommendation': rec.get('textualRecommendation', ''),
                'Summary': rec.get('implication', ''),
                'Avoid': 0, 'Alternate': 0, 'Dosing': 0 # Placeholders
            })

    if merge_rows:
        df_guidelines = pd.DataFrame(merge_rows)
        df_guidelines.to_sql('GuidelineMerge', conn, if_exists='replace', index=False)

def main():
    print("INICIANDO PROCESO DE REVISIÓN DE DATOS")
    print(f"Los archivos se guardarán en: {output_dir}\n")

    # 1. Procesar ClinAnn
    df_clin = actualizar_clinaan()
    if not df_clin.empty:
        archivo_clin = f"{output_dir}ClinAnn_Review.csv"
        df_clin.to_csv(archivo_clin, index=False)
        print(f"✅ ClinAnn generado exitosamente: {archivo_clin} ({len(df_clin)} registros)")
        # Imprimir muestra para verificación rápida en consola
        print(df_clin[['CAID', 'Gene', 'EvidenceCount', 'LevelModifier']].head())
    else:
        print("❌ Error: No se generaron datos para ClinAnn.")

    print("\n------------------------------------------------\n")

    # 2. Procesar Guidelines
    df_guide = actualizar_guidelines()
    if not df_guide.empty:
        archivo_guide = f"{output_dir}GuidelineMerge_Review.csv"
        df_guide.to_csv(archivo_guide, index=False)
        print(f"✅ GuidelineMerge generado exitosamente: {archivo_guide} ({len(df_guide)} registros)")
        print(df_guide[['Source', 'Gene', 'Recommendation']].head())
    else:
        print("❌ Error: No se generaron datos para Guidelines.")

if __name__ == "__main__":
    main()
