/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.operacoescomerciais;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author gabriela.gondo
 * Transações por mercadoria
 */
public class Informacao4a {
    
    //Mapper<Object, FORMATO_ENTRADA, FORMATO_CHAVE, FORMATO_VALOR>
    public static class MapperImplementacao4 extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String [] campos = linha.split(";");
            
            if(campos.length == 10){
                int ocorrencias = 1;
                String mercadoria = campos[3];
                context.write(new Text(mercadoria), new IntWritable(ocorrencias));
            }

        }
    }
  

    //Reducer<FORMATO_CHAVE_IN, FORMATO_VALOR_IN, FORMATO_CHAVE_OUT, FORMATO_VALOR_OUT>
    public static class ReducerImplementacao4 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for(IntWritable val : valores){
                soma += val.get();
            }
            context.write(chave, new IntWritable(soma));
        }

    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2021/SEM1/gabriela.gondo/Desktop/ImplementacaoLocalMR/OperacoesComerciais/Informacao4/TransacoesPorMercadoria";
        
        //Caso seja passado arquivos externamente, reescrevendo o valor das variáveis
        //Caso contrário, será feito teste local
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade4");
        
        job.setJarByClass(Informacao4a.class);
        job.setMapperClass(MapperImplementacao4.class); //Indicando a classe Mapper
        job.setReducerClass(ReducerImplementacao4.class); //Indicando a classe Reducer
        job.setOutputKeyClass(Text.class); //Formato de saída da chave
        job.setOutputValueClass(IntWritable.class); //Formato de saída do valor
        
        //Arquivo de entrada e saída
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        //Submetendo a tarefa ao MapReduce
        job.waitForCompletion(true);
    }
}
