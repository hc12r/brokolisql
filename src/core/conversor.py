# import pandas as pd
# import csv
# import sys

# def csv_to_sql(input_file, output_file):
#     # Verifica a extensão do arquivo de entrada
#     if input_file.endswith('.csv'):
#         df = pd.read_csv(input_file)
#     elif input_file.endswith('.xlsx'):
#         df = pd.read_excel(input_file)
#     else:
#         print("Formato de arquivo não suportado. Apenas arquivos CSV e Excel (XLSX) são permitidos.")
#         return

#     # Abre o arquivo de saída para escrever os comandos SQL
#     with open(output_file, 'w') as sql_file:
#         writer = csv.writer(sql_file)

#         # Itera sobre as linhas do DataFrame e escreve comandos SQL de inserção
#         for index, row in df.iterrows():
#             columns = ', '.join(row.index)
#             values = ', '.join(f"'{str(value)}'" for value in row.values)
#             insert_command = f"INSERT INTO tabela ({columns}) VALUES ({values});"
#             writer.writerow([insert_command])

#     print(f"Comandos SQL foram gerados e salvos em {output_file}")

# if __name__ == "__main__":
#     if len(sys.argv) != 3:
#         print("Uso: python3 tab_sql.py input_file output_file")
#         sys.exit(1)

#     input_file = sys.argv[1]
#     output_file = sys.argv[2]

#     csv_to_sql(input_file, output_file)
