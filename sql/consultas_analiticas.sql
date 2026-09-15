-- ================================================================
-- TECH CHALLENGE — FASE 3
-- Consultas SQL utilizadas no projeto
-- Fonte: histórico real de consultas executadas no Amazon Athena.
-- ================================================================
--
-- Este arquivo reúne somente consultas relevantes ao desenvolvimento
-- e aos resultados apresentados, removendo testes com erro, consultas
-- exploratórias de schema e execuções duplicadas.
--
-- Observação:
-- As análises de remuneração, modelo de trabalho e região x senioridade
-- foram desenvolvidas no Glue Notebook/Spark e, por isso, não aparecem
-- como consultas independentes neste histórico do Athena.
-- ================================================================


-- ================================================================
-- 01. Banco e validação dos dados
-- ================================================================

-- Execução original no histórico do Athena: 353b9f56-34fd-4689-83fb-b8fa3918c94a
CREATE DATABASE state_of_data

-- Execução original no histórico do Athena: 55017325-bc6b-43d8-9cd6-9c1fcaf88bc5
SELECT COUNT(*) AS total_registros
FROM tb_2023

-- Execução original no histórico do Athena: 1e8cfa33-c485-4aa6-a277-5d93c9224667
SELECT COUNT(*) AS total_registros
FROM tb_2024

-- Execução original no histórico do Athena: 694d3994-982e-4c68-a363-da32ca79d394
SELECT COUNT(*) AS total_registros
FROM tb_2025

-- Execução original no histórico do Athena: fbfe0b7e-9582-4e6f-b718-a4b007e9b9b6
SELECT
    ano,
    COUNT(*) AS respondentes
FROM gold_consolidada
GROUP BY ano
ORDER BY ano

-- ================================================================
-- 02. Criação das tabelas Gold
-- ================================================================

-- Execução original no histórico do Athena: 93d539c1-66b0-45ba-9b78-d4e33edeff78
CREATE TABLE gold_2023
WITH (
    format = 'PARQUET',
    external_location = 's3://lab-385615870279/data-output/gold/2023/'
) AS

SELECT
    2023 AS ano,

    -- Perfil
    "_'p1_a_'__'idade'_#1" AS idade,
    "_'p1_b_'__'genero'_#3" AS genero,

    -- Localização
    "_'p1_i_'__'estado_onde_mora'_#22" AS estado,
    "_'p1_i_1_'__'uf_onde_mora'_#23" AS uf,
    "_'p1_i_2_'__'regiao_onde_mora'_#24" AS regiao,

    -- Perfil profissional
    "_'p2_b_'__'setor'_#30" AS setor,
    "_'p2_f_'__'cargo_atual'_#34" AS cargo,
    "_'p2_g_'__'nivel'_#35" AS nivel,

    -- Remuneração e trabalho
    "_'p2_h_'__'faixa_salarial'_#36" AS faixa_salarial,
    "_'p2_r_'__'atualmente_qual_a_sua_forma_de_trabalho?'_#62" AS modelo_trabalho,

    -- IA Generativa
    "_'p3_f_1_'__'colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada'_#105"
        AS ia_uso_independente,

    "_'p3_f_2_'__'direcionamento_centralizado_do_uso_de_ai_generativa'_#106"
        AS ia_uso_centralizado,

    "_'p3_f_3_'__'desenvolvedores_utilizando_copilots'_#107"
        AS ia_copilots,

    "_'p3_f_4_'__'ai_generativa_e_llms_para_melhorar_produtos_externos'_#108"
        AS ia_produtos_externos,

    "_'p3_f_5_'__'ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores'_#109"
        AS ia_produtos_internos,

    "_'p3_f_6_'__'ia_generativa_e_llms_como_principal_frente_do_negócio'_#110"
        AS ia_principal_frente,

    "_'p3_f_7_'__'ia_generativa_e_llms_não_é_prioridade'_#111"
        AS ia_nao_prioridade,

    "_'p3_f_8_'__'não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa'_#112"
        AS ia_nao_sei_opinar,

    -- Linguagens
    "_'p4_d_1_'__'sql'_#144" AS linguagem_sql,
    "_'p4_d_2_'__'r_'_#145" AS linguagem_r,
    "_'p4_d_3_'__'python'_#146" AS linguagem_python,
    "_'p4_d_4_'__'c/c++/c#'_#147" AS linguagem_c_cpp_csharp,
    "_'p4_d_5_'__'.net'_#148" AS linguagem_dotnet,
    "_'p4_d_6_'__'java'_#149" AS linguagem_java,
    "_'p4_d_7_'__'julia'_#150" AS linguagem_julia,
    "_'p4_d_8_'__'sas/stata'_#151" AS linguagem_sas_stata,
    "_'p4_d_9_'__'visual_basic/vba'_#152" AS linguagem_vba,
    "_'p4_d_10_'__'scala'_#153" AS linguagem_scala,
    "_'p4_d_11_'__'matlab'_#154" AS linguagem_matlab,
    "_'p4_d_12_'__'rust'_#155" AS linguagem_rust,
    "_'p4_d_13_'__'php'_#156" AS linguagem_php,
    "_'p4_d_14_'__'javascript'_#157" AS linguagem_javascript,
    "_'p4_d_15_'__'não_utilizo_nenhuma_linguagem'_#158"
        AS linguagem_nenhuma

FROM tb_2023

-- Execução original no histórico do Athena: b4bffc58-f42c-4651-94b9-53eb20f03555
CREATE TABLE gold_2024
WITH (
    format = 'PARQUET',
    external_location = 's3://lab-385615870279/data-output/gold/2024/'
) AS

SELECT
    2024 AS ano,

    -- Perfil
    "1.a_idade" AS idade,
    "1.b_genero" AS genero,

    -- Localização
    "1.i_estado_onde_mora" AS estado,
    "1.i.1_uf_onde_mora" AS uf,
    "1.i.2_regiao_onde_mora" AS regiao,

    -- Perfil profissional
    "2.b_setor" AS setor,
    "2.f_cargo_atual" AS cargo,
    "2.g_nivel" AS nivel,

    -- Remuneração e trabalho
    "2.h_faixa_salarial" AS faixa_salarial,
    "2.r_modelo_de_trabalho_atual" AS modelo_trabalho,

    -- IA Generativa
    "3.f.1_colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada#65"
        AS ia_uso_independente,

    "3.f.2_direcionamento_centralizado_do_uso_de_ai_generativa#66"
        AS ia_uso_centralizado,

    "3.f.3_desenvolvedores_utilizando_copilots#67"
        AS ia_copilots,

    "3.f.4_ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais#68"
        AS ia_produtos_externos,

    "3.f.5_ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores#69"
        AS ia_produtos_internos,

    "3.f.6_ia_generativa_e_llms_como_principal_frente_do_negócio#70"
        AS ia_principal_frente,

    "3.f.7_ia_generativa_e_llms_não_é_prioridade#71"
        AS ia_nao_prioridade,

    "3.f.8_não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa#72"
        AS ia_nao_sei_opinar,

    -- Linguagens
    "4.d.1_sql" AS linguagem_sql,
    "4.d.2_r" AS linguagem_r,
    "4.d.3_python" AS linguagem_python,
    "4.d.4_c/c++/c#" AS linguagem_c_cpp_csharp,
    "4.d.5_.net" AS linguagem_dotnet,
    "4.d.6_java" AS linguagem_java,
    "4.d.7_julia" AS linguagem_julia,
    "4.d.8_sas/stata" AS linguagem_sas_stata,
    "4.d.9_visual_basic/vba#90" AS linguagem_vba,
    "4.d.10_scala" AS linguagem_scala,
    "4.d.11_matlab" AS linguagem_matlab,
    "4.d.12_rust" AS linguagem_rust,
    "4.d.13_php" AS linguagem_php,
    "4.d.14_javascript" AS linguagem_javascript,
    "4.d.15_não_utilizo_nenhuma_das_linguagens_listadas#91" AS linguagem_nenhuma

FROM tb_2024

-- Execução original no histórico do Athena: 9c251af4-ce4f-4304-a88c-2ec2494e8e11
CREATE TABLE gold_2025
WITH (
    format = 'PARQUET',
    external_location = 's3://lab-385615870279/data-output/gold/2025/'
) AS

SELECT
    2025 AS ano,

    -- Perfil
    "1.a_idade" AS idade,
    "1.b_genero" AS genero,

    -- Localização
    "1.i_estado_onde_mora" AS estado,
    "1.i.1_uf_onde_mora" AS uf,
    "1.i.2_regiao_onde_mora" AS regiao,

    -- Perfil profissional
    "2.b_setor" AS setor,
    "2.f_cargo_atual" AS cargo,
    "2.g_nivel" AS nivel,

    -- Remuneração e trabalho
    "2.h_faixa_salarial" AS faixa_salarial,
    "2.q_modelo_de_trabalho_atual" AS modelo_trabalho,

    -- IA Generativa
    "3.f.1_colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada#65"
        AS ia_uso_independente,

    "3.f.2_direcionamento_centralizado_do_uso_de_ai_generativa#66"
        AS ia_uso_centralizado,

    "3.f.3_desenvolvedores_utilizando_copilots#67"
        AS ia_copilots,

    "3.f.4_ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais#68"
        AS ia_produtos_externos,

    "3.f.5_ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores#69"
        AS ia_produtos_internos,

    "3.f.6_ia_generativa_e_llms_como_principal_frente_do_negócio#70"
        AS ia_principal_frente,

    "3.f.7_ia_generativa_e_llms_não_é_prioridade#71"
        AS ia_nao_prioridade,

    "3.f.8_não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa#72"
        AS ia_nao_sei_opinar,

    -- Linguagens
    "4.c.1_sql" AS linguagem_sql,
    "4.c.2_r" AS linguagem_r,
    "4.c.3_python" AS linguagem_python,
    "4.c.4_c/c++/c#" AS linguagem_c_cpp_csharp,
    "4.c.5_julia" AS linguagem_julia,
    "4.c.6_visual_basic/vba#86" AS linguagem_vba,
    "4.c.7_scala" AS linguagem_scala,
    "4.c.8_dax" AS linguagem_dax,
    "4.c.9_rust" AS linguagem_rust,
    "4.c.10_não_utilizo_nenhuma_das_linguagens_listadas#87" AS linguagem_nenhuma

FROM tb_2025

-- Execução original no histórico do Athena: 57a43b0b-5f06-49e3-8e7f-46f3c370ec71
CREATE TABLE gold_consolidada
WITH (
    format = 'PARQUET',
    external_location = 's3://lab-385615870279/data-output/gold/consolidada/'
) AS

SELECT
    ano,
    idade,
    genero,
    estado,
    uf,
    regiao,
    setor,
    cargo,
    nivel,
    faixa_salarial,
    modelo_trabalho,

    ia_uso_independente,
    ia_uso_centralizado,
    ia_copilots,
    ia_produtos_externos,
    ia_produtos_internos,
    ia_principal_frente,
    ia_nao_prioridade,
    ia_nao_sei_opinar,

    linguagem_sql,
    linguagem_r,
    linguagem_python,
    linguagem_c_cpp_csharp,
    linguagem_dotnet,
    linguagem_java,
    linguagem_julia,
    linguagem_sas_stata,
    linguagem_vba,
    linguagem_scala,
    linguagem_matlab,
    linguagem_rust,
    linguagem_php,
    linguagem_javascript,

    CAST(NULL AS VARCHAR) AS linguagem_dax,

    linguagem_nenhuma

FROM gold_2023

UNION ALL

SELECT
    ano,
    idade,
    genero,
    estado,
    uf,
    regiao,
    setor,
    cargo,
    nivel,
    faixa_salarial,
    modelo_trabalho,

    ia_uso_independente,
    ia_uso_centralizado,
    ia_copilots,
    ia_produtos_externos,
    ia_produtos_internos,
    ia_principal_frente,
    ia_nao_prioridade,
    ia_nao_sei_opinar,

    linguagem_sql,
    linguagem_r,
    linguagem_python,
    linguagem_c_cpp_csharp,
    linguagem_dotnet,
    linguagem_java,
    linguagem_julia,
    linguagem_sas_stata,
    linguagem_vba,
    linguagem_scala,
    linguagem_matlab,
    linguagem_rust,
    linguagem_php,
    linguagem_javascript,

    CAST(NULL AS VARCHAR) AS linguagem_dax,

    linguagem_nenhuma

FROM gold_2024

UNION ALL

SELECT
    ano,
    idade,
    genero,
    estado,
    uf,
    regiao,
    setor,
    cargo,
    nivel,
    faixa_salarial,
    modelo_trabalho,

    ia_uso_independente,
    ia_uso_centralizado,
    ia_copilots,
    ia_produtos_externos,
    ia_produtos_internos,
    ia_principal_frente,
    ia_nao_prioridade,
    ia_nao_sei_opinar,

    linguagem_sql,
    linguagem_r,
    linguagem_python,
    linguagem_c_cpp_csharp,

    CAST(NULL AS VARCHAR) AS linguagem_dotnet,
    CAST(NULL AS VARCHAR) AS linguagem_java,

    linguagem_julia,

    CAST(NULL AS VARCHAR) AS linguagem_sas_stata,

    linguagem_vba,
    linguagem_scala,

    CAST(NULL AS VARCHAR) AS linguagem_matlab,

    linguagem_rust,

    CAST(NULL AS VARCHAR) AS linguagem_php,
    CAST(NULL AS VARCHAR) AS linguagem_javascript,

    linguagem_dax,

    linguagem_nenhuma

FROM gold_2025

-- ================================================================
-- 03. Q1 — Estrutura do mercado
-- ================================================================

-- Execução original no histórico do Athena: a1d34803-1501-49f7-94b1-5ac8a19856cd
SELECT
    ano,
    nivel,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY ano),
        2
    ) AS percentual
FROM gold_consolidada
WHERE nivel IS NOT NULL
  AND TRIM(nivel) <> ''
GROUP BY
    ano,
    nivel
ORDER BY
    ano,
    quantidade DESC

-- Execução original no histórico do Athena: 7104dedb-8f83-491f-8aec-d3afbfa14931
SELECT
    ano,
    cargo,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY ano),
        2
    ) AS percentual
FROM gold_consolidada
WHERE cargo IS NOT NULL
  AND TRIM(cargo) <> ''
GROUP BY
    ano,
    cargo
ORDER BY
    ano,
    quantidade DESC

-- Execução original no histórico do Athena: 8e34576f-733f-4b96-8b86-787f41a8bbc5
SELECT
    ano,
    setor,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (PARTITION BY ano),
        2
    ) AS percentual
FROM gold_consolidada
WHERE setor IS NOT NULL
  AND TRIM(setor) <> ''
GROUP BY
    ano,
    setor
ORDER BY
    ano,
    quantidade DESC

-- ================================================================
-- 04. Q3 — Diversidade de gênero
-- ================================================================

-- Execução original no histórico do Athena: 185529a6-db51-415f-beaa-f7b4f5c8ccc7
SELECT
    "_'p1_b_'__'genero'_#3" AS genero,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentual
FROM tb_2023
GROUP BY "_'p1_b_'__'genero'_#3"
ORDER BY quantidade DESC

-- Execução original no histórico do Athena: f485deaa-9593-4322-bde8-0110d02a501a
SELECT
    "1.b_genero" AS genero,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentual
FROM tb_2024
GROUP BY "1.b_genero"
ORDER BY quantidade DESC

-- Execução original no histórico do Athena: 51a3b8c4-eaaa-4391-ab63-3a56726c3179
SELECT
    "1.b_genero" AS genero,
    COUNT(*) AS quantidade,
    ROUND(
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
        2
    ) AS percentual
FROM tb_2025
GROUP BY "1.b_genero"
ORDER BY quantidade DESC

-- ================================================================
-- 05. Q4 — Tecnologias: linguagens
-- ================================================================

-- Execução original no histórico do Athena: 537aeddc-117c-4c82-b7dd-9b4f7e363611
SELECT
    COUNT(*) AS total,

    SUM(CASE WHEN "_'p4_d_1_'__'sql'_#144" = '1' THEN 1 ELSE 0 END) AS sql,

    SUM(CASE WHEN "_'p4_d_2_'__'r_'_#145" = '1' THEN 1 ELSE 0 END) AS r,

    SUM(CASE WHEN "_'p4_d_3_'__'python'_#146" = '1' THEN 1 ELSE 0 END) AS python,

    SUM(CASE WHEN "_'p4_d_4_'__'c/c++/c#'_#147" = '1' THEN 1 ELSE 0 END) AS c_cpp_csharp,

    SUM(CASE WHEN "_'p4_d_5_'__'.net'_#148" = '1' THEN 1 ELSE 0 END) AS dotnet,

    SUM(CASE WHEN "_'p4_d_6_'__'java'_#149" = '1' THEN 1 ELSE 0 END) AS java,

    SUM(CASE WHEN "_'p4_d_7_'__'julia'_#150" = '1' THEN 1 ELSE 0 END) AS julia,

    SUM(CASE WHEN "_'p4_d_8_'__'sas/stata'_#151" = '1' THEN 1 ELSE 0 END) AS sas_stata,

    SUM(CASE WHEN "_'p4_d_9_'__'visual_basic/vba'_#152" = '1' THEN 1 ELSE 0 END) AS visual_basic_vba,

    SUM(CASE WHEN "_'p4_d_10_'__'scala'_#153" = '1' THEN 1 ELSE 0 END) AS scala,

    SUM(CASE WHEN "_'p4_d_11_'__'matlab'_#154" = '1' THEN 1 ELSE 0 END) AS matlab,

    SUM(CASE WHEN "_'p4_d_12_'__'rust'_#155" = '1' THEN 1 ELSE 0 END) AS rust,

    SUM(CASE WHEN "_'p4_d_13_'__'php'_#156" = '1' THEN 1 ELSE 0 END) AS php,

    SUM(CASE WHEN "_'p4_d_14_'__'javascript'_#157" = '1' THEN 1 ELSE 0 END) AS javascript,

    SUM(CASE WHEN "_'p4_d_15_'__'não_utilizo_nenhuma_linguagem'_#158" = '1' THEN 1 ELSE 0 END) AS nenhuma

FROM tb_2023

-- Execução original no histórico do Athena: a25a0deb-d24b-4c8b-8543-2f9041fdcaf1
SELECT
    COUNT(*) AS total,

    SUM(CASE WHEN "4.d.1_sql" = '1' THEN 1 ELSE 0 END) AS sql,

    SUM(CASE WHEN "4.d.2_r" = '1' THEN 1 ELSE 0 END) AS r,

    SUM(CASE WHEN "4.d.3_python" = '1' THEN 1 ELSE 0 END) AS python,

    SUM(CASE WHEN "4.d.4_c/c++/c#" = '1' THEN 1 ELSE 0 END) AS c_cpp_csharp,

    SUM(CASE WHEN "4.d.5_.net" = '1' THEN 1 ELSE 0 END) AS dotnet,

    SUM(CASE WHEN "4.d.6_java" = '1' THEN 1 ELSE 0 END) AS java,

    SUM(CASE WHEN "4.d.7_julia" = '1' THEN 1 ELSE 0 END) AS julia,

    SUM(CASE WHEN "4.d.8_sas/stata" = '1' THEN 1 ELSE 0 END) AS sas_stata,

    SUM(CASE WHEN "4.d.9_visual_basic/vba#90" = '1' THEN 1 ELSE 0 END) AS visual_basic_vba,

    SUM(CASE WHEN "4.d.10_scala" = '1' THEN 1 ELSE 0 END) AS scala,

    SUM(CASE WHEN "4.d.11_matlab" = '1' THEN 1 ELSE 0 END) AS matlab,

    SUM(CASE WHEN "4.d.12_rust" = '1' THEN 1 ELSE 0 END) AS rust,

    SUM(CASE WHEN "4.d.13_php" = '1' THEN 1 ELSE 0 END) AS php,

    SUM(CASE WHEN "4.d.14_javascript" = '1' THEN 1 ELSE 0 END) AS javascript,

    SUM(CASE WHEN "4.d.15_não_utilizo_nenhuma_das_linguagens_listadas#91" = '1' THEN 1 ELSE 0 END) AS nenhuma

FROM tb_2024

-- Execução original no histórico do Athena: 3aeb629f-f2d6-4254-8f5a-76226d3663b3
SELECT
    COUNT(*) AS total,

    SUM(CASE WHEN "4.c.1_sql" = '1' THEN 1 ELSE 0 END) AS sql,

    SUM(CASE WHEN "4.c.2_r" = '1' THEN 1 ELSE 0 END) AS r,

    SUM(CASE WHEN "4.c.3_python" = '1' THEN 1 ELSE 0 END) AS python,

    SUM(CASE WHEN "4.c.4_c/c++/c#" = '1' THEN 1 ELSE 0 END) AS c_cpp_csharp,

    SUM(CASE WHEN "4.c.5_julia" = '1' THEN 1 ELSE 0 END) AS julia,

    SUM(CASE WHEN "4.c.6_visual_basic/vba#86" = '1' THEN 1 ELSE 0 END) AS visual_basic_vba,

    SUM(CASE WHEN "4.c.7_scala" = '1' THEN 1 ELSE 0 END) AS scala,

    SUM(CASE WHEN "4.c.8_dax" = '1' THEN 1 ELSE 0 END) AS dax,

    SUM(CASE WHEN "4.c.9_rust" = '1' THEN 1 ELSE 0 END) AS rust,

    SUM(CASE WHEN "4.c.10_não_utilizo_nenhuma_das_linguagens_listadas#87" = '1' THEN 1 ELSE 0 END) AS nenhuma

FROM tb_2025

-- ================================================================
-- 06. Q5 — Inteligência Artificial
-- ================================================================

-- Execução original no histórico do Athena: e96103dc-b17a-488a-8a65-cf24a67cec00
SELECT
    COUNT(*) AS total,

    SUM(CASE WHEN "_'p3_f_1_'__'colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada'_#105" = '1' THEN 1 ELSE 0 END) AS uso_independente,

    SUM(CASE WHEN "_'p3_f_2_'__'direcionamento_centralizado_do_uso_de_ai_generativa'_#106" = '1' THEN 1 ELSE 0 END) AS uso_centralizado,

    SUM(CASE WHEN "_'p3_f_3_'__'desenvolvedores_utilizando_copilots'_#107" = '1' THEN 1 ELSE 0 END) AS uso_copilots,

    SUM(CASE WHEN "_'p3_f_4_'__'ai_generativa_e_llms_para_melhorar_produtos_externos'_#108" = '1' THEN 1 ELSE 0 END) AS produtos_externos,

    SUM(CASE WHEN "_'p3_f_5_'__'ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores'_#109" = '1' THEN 1 ELSE 0 END) AS produtos_internos,

    SUM(CASE WHEN "_'p3_f_6_'__'ia_generativa_e_llms_como_principal_frente_do_negócio'_#110" = '1' THEN 1 ELSE 0 END) AS principal_frente_negocio,

    SUM(CASE WHEN "_'p3_f_7_'__'ia_generativa_e_llms_não_é_prioridade'_#111" = '1' THEN 1 ELSE 0 END) AS nao_prioridade,

    SUM(CASE WHEN "_'p3_f_8_'__'não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa'_#112" = '1' THEN 1 ELSE 0 END) AS nao_sei_opinar

FROM tb_2023

-- Execução original no histórico do Athena: 99f7102d-a8f3-41d6-b67d-b97a4ff65b75
SELECT
    COUNT(*) AS total,

    SUM(CASE 
        WHEN "3.f.1_colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada#65" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_independente,

    SUM(CASE 
        WHEN "3.f.2_direcionamento_centralizado_do_uso_de_ai_generativa#66" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_centralizado,

    SUM(CASE 
        WHEN "3.f.3_desenvolvedores_utilizando_copilots#67" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_copilots,

    SUM(CASE 
        WHEN "3.f.4_ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais#68" = '1' 
        THEN 1 ELSE 0 
    END) AS produtos_externos,

    SUM(CASE 
        WHEN "3.f.5_ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores#69" = '1' 
        THEN 1 ELSE 0 
    END) AS produtos_internos,

    SUM(CASE 
        WHEN "3.f.6_ia_generativa_e_llms_como_principal_frente_do_negócio#70" = '1' 
        THEN 1 ELSE 0 
    END) AS principal_frente_negocio,

    SUM(CASE 
        WHEN "3.f.7_ia_generativa_e_llms_não_é_prioridade#71" = '1' 
        THEN 1 ELSE 0 
    END) AS nao_prioridade,

    SUM(CASE 
        WHEN "3.f.8_não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa#72" = '1' 
        THEN 1 ELSE 0 
    END) AS nao_sei_opinar

FROM tb_2024

-- Execução original no histórico do Athena: d6cd5ad9-4ba7-4c00-bfe5-16b9dce8e37b
SELECT
    COUNT(*) AS total,

    SUM(CASE 
        WHEN "3.f.1_colaboradores_usando_ai_generativa_de_forma_independente_e_descentralizada#65" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_independente,

    SUM(CASE 
        WHEN "3.f.2_direcionamento_centralizado_do_uso_de_ai_generativa#66" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_centralizado,

    SUM(CASE 
        WHEN "3.f.3_desenvolvedores_utilizando_copilots#67" = '1' 
        THEN 1 ELSE 0 
    END) AS uso_copilots,

    SUM(CASE 
        WHEN "3.f.4_ai_generativa_e_llms_para_melhorar_produtos_externos_para_os_clientes_finais#68" = '1' 
        THEN 1 ELSE 0 
    END) AS produtos_externos,

    SUM(CASE 
        WHEN "3.f.5_ai_generativa_e_llms_para_melhorar_produtos_internos_para_os_colaboradores#69" = '1' 
        THEN 1 ELSE 0 
    END) AS produtos_internos,

    SUM(CASE 
        WHEN "3.f.6_ia_generativa_e_llms_como_principal_frente_do_negócio#70" = '1' 
        THEN 1 ELSE 0 
    END) AS principal_frente_negocio,

    SUM(CASE 
        WHEN "3.f.7_ia_generativa_e_llms_não_é_prioridade#71" = '1' 
        THEN 1 ELSE 0 
    END) AS nao_prioridade,

    SUM(CASE 
        WHEN "3.f.8_não_sei_opinar_sobre_o_uso_de_ia_generativa_e_llms_na_empresa#72" = '1' 
        THEN 1 ELSE 0 
    END) AS nao_sei_opinar

FROM tb_2025
