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
 * Mercadoria de maior peso
 */
public class Informacao7b {
    
    //Mapper<Object, FORMATO_ENTRADA, FORMATO_CHAVE, FORMATO_VALOR>
    public static class MapperImplementacao7 extends Mapper<Object, Text, Text, IntWritable>{
        int maior = 0;
        String chaveMaior;
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String [] campos = linha.split("	");
            
            if(campos.length == 2){
                String mercadoria = campos[0];
                int ocorrencias = Integer.parseInt(campos[1]);
                
                if(ocorrencias > maior){
                    chaveMaior = mercadoria;
                    maior = ocorrencias;
                }
            }

        }
        
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text(chaveMaior), new IntWritable(maior));
        }
    }
  

    //Reducer<FORMATO_CHAVE_IN, FORMATO_VALOR_IN, FORMATO_CHAVE_OUT, FORMATO_VALOR_OUT>
    public static class ReducerImplementacao7 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            for(IntWritable val : valores){
                context.write(chave, val);
            }
        }

    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home2/ead2021/SEM1/gabriela.gondo/Desktop/ImplementacaoLocalMR/OperacoesComerciais/Informacao7/TransacoesPorMercadoriaPeso/part-r-00000";
        String arquivoSaida = "/home2/ead2021/SEM1/gabriela.gondo/Desktop/ImplementacaoLocalMR/OperacoesComerciais/Informacao7/MercadoriaDeMaiorPeso";
        
        //Caso seja passado arquivos externamente, reescrevendo o valor das variáveis
        //Caso contrário, será feito teste local
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade7");
        
        job.setJarByClass(Informacao7b.class);
        job.setMapperClass(MapperImplementacao7.class); //Indicando a classe Mapper
        job.setReducerClass(ReducerImplementacao7.class); //Indicando a classe Reducer
        job.setOutputKeyClass(Text.class); //Formato de saída da chave
        job.setOutputValueClass(IntWritable.class); //Formato de saída do valor
        
        //Arquivo de entrada e saída
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        //Submetendo a tarefa ao MapReduce
        job.waitForCompletion(true);
    }
}
