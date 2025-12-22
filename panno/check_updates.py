import requests
import os
import zipfile
import pandas as pd
import io
import json
import sqlite3

url = "https://api.pharmgkb.org/v1/download/file/data/clinicalAnnotations.zip"
url_api = "https://api.pharmgkb.org/v1"

temp_dir = "./panno/data/temp/"
os.makedirs(temp_dir, exist_ok=True)

output_dir = "./panno/data/output_review/"
os.makedirs(output_dir, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

#Generar ClinAnn
def actualizar_clinaan():
    try:
        print(f"⬇️ Iniciando descarga de ClinAnn desde: {url}...")
        response = requests.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()  # Raise an error for bad status codes
        print("✅ Descarga completada.")

        print("📦 Descomprimiendo archivos...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extract("clinical_annotations.tsv", temp_dir)
            z.extract("clinical_ann_alleles.tsv", temp_dir)
            #z.extract("clinical_ann_evidence.tsv", temp_dir)
        
        print("📊 Leyendo archivos CSV...")
        df_meta = pd.read_csv(f"{temp_dir}clinical_annotations.tsv", sep="\t")
        df_alleles = pd.read_csv(f"{temp_dir}clinical_ann_alleles.tsv", sep="\t")
        #df_evidence = pd.read_csv(f"{temp_dir}clinical_ann_evidence.tsv", sep="\t")

        print("🔄 Procesando y uniendo datos (esto puede tardar un poco)...")
        merged = pd.merge(df_alleles, df_meta, on="Clinical Annotation ID", how="inner")

        def split_alleles(g):
            if pd.isna(g) or len(g) != 2: return g, None # Fallback simple
            return g[0], g[1]

        print("🧬 Dividiendo alelos...")
        merged[['Allele1', 'Allele2']] = merged['Genotype/Allele'].apply(
            lambda x: pd.Series(split_alleles(x))
        )

        print("📝 Construyendo DataFrame final...")
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
        df_final['PMIDCount'] = merged["PMID Count"]
        df_final['EvidenceCount'] = merged["Evidence Count"]
        df_final['Specialty'] = merged["Specialty Population"]
        df_final['PhenotypeCategory'] = merged['Phenotype Category']
        df_final.fillna("", inplace=True)

        return df_final

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while downloading PharmGKB guidelines: {e}")

def actualizar_guidelines():
    # URL para descargar annotations en formato JSON (el ZIP contiene muchos JSONs)
    # Nota: La API devuelve 303 Redirect a S3, requests lo maneja automticamente.
    url_guidelines_zip = "https://api.pharmgkb.org/v1/download/file/data/guidelineAnnotations.json.zip"
    
    try:
        print(f"⬇️ Descargando archivo masivo de Guidelines (JSON) desde: {url_guidelines_zip}...")
        response = requests.get(url_guidelines_zip, headers=HEADERS, timeout=60)
        response.raise_for_status()

        print("📦 Descomprimiendo archivos JSON...")
        # Limpiamos temp_dir de JSONs previos para evitar mezclas
        for f in os.listdir(temp_dir):
            if f.endswith(".json"):
                os.remove(os.path.join(temp_dir, f))

        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(temp_dir)
        
        print("📊 Procesando archivos JSON de Guidelines...")
        
        data_rows = []
        # Iteramos sobre todos los archivos JSON extraídos
        json_files = [f for f in os.listdir(temp_dir) if f.endswith(".json")]
        
        target_sources = ['CPIC', 'DPWG', 'RNPGx']
        
        for json_file in json_files:
            try:
                with open(os.path.join(temp_dir, json_file), 'r', encoding='utf-8') as f:
                    d = json.load(f)
                    
                    # Estructura típica: d['guideline'] contiene la info
                    g = d.get('guideline', {})
                    source = g.get('source', '')
                    
                    if source not in target_sources:
                        continue
                        
                    # Extraer campos clave
                    guideline_id = g.get('id', '')
                    
                    # Summary y Recommendation vienen a veces en summaryMarkdown (objeto o string)
                    summary_obj = g.get('summaryMarkdown', {})
                    summary_text = ""
                    if isinstance(summary_obj, dict):
                        summary_text = summary_obj.get('html', '')
                    else:
                        summary_text = str(summary_obj)
                    
                    summary_text = summary_text.replace('\n', ' ').strip()
                    
                    # Genes (puede haber varios, tomamos los símbolos unidos)
                    genes = [x.get('symbol', '') for x in g.get('relatedGenes', [])]
                    gene_str = "; ".join(filter(None, genes))
                    
                    # Drugs
                    drugs = [x.get('name', '') for x in g.get('relatedChemicals', [])]
                    drug_str = "; ".join(filter(None, drugs))

                    # Mapeo a columnas de GuidelineMerge (Schema Match)
                    row = {
                        # 'ID': ser asignado después numéricamente
                        'Source': source,
                        'PAID': guideline_id, # ID original de PharmGKB va aquí
                        'Summary': g.get('name', ''), # Título como summary
                        'Phenotype': "", 
                        'Genotype': "", # Schema expects this
                        'Recommendation': summary_text, 
                        'Avoid': 0, # Default
                        'Alternate': 1 if g.get('alternateDrugAvailable') else 0,
                        'Dosing': 1 if g.get('dosingInformation') else 0,
                        'Gene': gene_str,
                        'Drug': drug_str,
                        'GeneID': 0, # Filler
                        'DrugID': 0  # Filler
                    }
                    data_rows.append(row)
                    
            except Exception as e:
                print(f"⚠️ Error leyendo {json_file}: {e}")
                continue

        df_final = pd.DataFrame(data_rows)
        
        # Generar ID numérico secuencial como espera la DB
        if not df_final.empty:
            df_final.insert(0, 'ID', range(1, len(df_final) + 1))

        print(f"✅ Procesados {len(df_final)} registros de {', '.join(target_sources)}.")
        
        if df_final.empty:
            return pd.DataFrame()

        # Rellenar vacíos
        df_final.fillna("", inplace=True)
        
        return df_final

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de conexión descargando Guidelines ZIP: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error procesando Guidelines: {e}")
        return pd.DataFrame()

def regenerar_guidelinerules(df_new_guidelines):
    """
    Genera GuidelineRule_Review.csv usando las reglas existentes en la DB
    pero actualizando los GuidelineID para que coincidan con los nuevos generados.
    """
    db_path = "./assets/pgx_kb.sqlite3"
    if not os.path.exists(db_path):
        print(f"⚠️ No se encontró la base de datos en {db_path}, no se puede regenerar GuidelineRule.")
        return pd.DataFrame()

    print(f"🔄 Migrando reglas de GuidelineRule desde {db_path}...")
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Leemos las reglas antiguas junto con el PAID de su guía original
        query = """
        SELECT GR.ID, GR.Gene, GR.Variant, GR.Allele1, GR.Allele2, GR.Phenotype, GR.ClinAnnID, GM.PAID
        FROM GuidelineRule GR
        JOIN GuidelineMerge GM ON GR.GuidelineID = GM.ID
        """
        df_rules_old = pd.read_sql_query(query, conn)
        conn.close()
        
        if df_rules_old.empty:
            print("⚠️ La tabla GuidelineRule antigua está vacía.")
            return pd.DataFrame()

        print(f"   Leídas {len(df_rules_old)} reglas antiguas.")
        
        # Hacemos merge con las NUEVAS guidelines usando 'PAID' como clave
        # df_new_guidelines tiene columnas: ID, Source, PAID, ...
        # Queremos pegar el nuevo 'ID' donde coincida el 'PAID'
        
        # Renombramos para claridad
        df_new_map = df_new_guidelines[['ID', 'PAID']].rename(columns={'ID': 'NewGuidelineID'})
        
        # Merge
        df_merged = pd.merge(df_rules_old, df_new_map, on='PAID', how='inner')
        
        print(f"   {len(df_merged)} reglas coincidieron con las nuevas Guidelines.")
        
        if df_merged.empty:
            print("⚠️ Ninguna regla coincidió (¿Cambiaron todos los IDs de PharmGKB?).")
            return pd.DataFrame()

        # Construimos el DF final
        df_rules_final = pd.DataFrame()
        df_rules_final['ID'] = range(1, len(df_merged) + 1) # Regeneramos ID secuencial
        df_rules_final['Gene'] = df_merged['Gene']
        df_rules_final['Variant'] = df_merged['Variant']
        df_rules_final['Allele1'] = df_merged['Allele1']
        df_rules_final['Allele2'] = df_merged['Allele2']
        df_rules_final['Phenotype'] = df_merged['Phenotype']
        df_rules_final['ClinAnnID'] = df_merged['ClinAnnID']
        df_rules_final['GuidelineID'] = df_merged['NewGuidelineID'] # El nuevo ID entero
        
        return df_rules_final
        
    except Exception as e:
        print(f"❌ Error migrando GuidelineRule: {e}")
        return pd.DataFrame()

def main():
    print("INICIANDO PROCESO DE REVISIÓN DE DATOS")
    print(f"Los archivos se guardarán en: {output_dir}\n")

    # 1. Procesar ClinAnn
    df_clin = actualizar_clinaan()
    
    if not df_clin.empty:
        archivo_clin = f"{output_dir}ClinAnn_Review.csv"
        df_clin.to_csv(archivo_clin, index=False)
        print(f"✅ ClinAnn generado exitosamente: {archivo_clin} ({len(df_clin)} registros)")
    else:
        print("⚠️ No se generaron datos para ClinAnn (Verifica errores arriba).")

    print("\n------------------------------------------------\n")

    # 2. Procesar Guidelines
    df_guide = actualizar_guidelines()
    
    if not df_guide.empty:
        archivo_guide = f"{output_dir}GuidelineMerge_Review.csv"
        df_guide.to_csv(archivo_guide, index=False)
        print(f"✅ GuidelineMerge generado exitosamente: {archivo_guide} ({len(df_guide)} registros)")

        # 3. Regenerar GuidelineRule (Solo si tenemos Guidelines nuevos)
        print("\n------------------------------------------------\n")
        df_rules = regenerar_guidelinerules(df_guide)
        
        if not df_rules.empty:
            archivo_rules = f"{output_dir}GuidelineRule_Review.csv"
            df_rules.to_csv(archivo_rules, index=False)
            print(f"✅ GuidelineRule generado exitosamente: {archivo_rules} ({len(df_rules)} registros)")
        else:
             print("⚠️ No se generaron datos para GuidelineRule.")

    else:
        print("⚠️ No se generaron datos para Guidelines.")


if __name__ == "__main__":
    main()