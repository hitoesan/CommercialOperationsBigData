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
 * Transações comerciais por peso
 */
public class Informacao7a {
    
    //Mapper<Object, FORMATO_ENTRADA, FORMATO_CHAVE, FORMATO_VALOR>
    public static class MapperImplementacao7 extends Mapper<Object, Text, Text, IntWritable>{
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String [] campos = linha.split(";");            
            IntWritable valorMap = new IntWritable(0);
            
            if(campos.length == 10){
                String peso = campos[6];
                String mercadoria = campos[3];
                
                try{
                    valorMap = new IntWritable(Integer.parseInt(peso));
                }catch(NumberFormatException e){
                    
                }finally{
                    
                }
                context.write(new Text(mercadoria), valorMap);
            }

        }
    }
  

    //Reducer<FORMATO_CHAVE_IN, FORMATO_VALOR_IN, FORMATO_CHAVE_OUT, FORMATO_VALOR_OUT>
    public static class ReducerImplementacao7 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            long soma = 0;
            IntWritable valorSaida= new IntWritable(0);
            
            for(IntWritable val : valores){
                soma += val.get();
            }
            
            try{
                valorSaida.set(Integer.parseInt(String.valueOf(soma)));
            }catch(NumberFormatException e){
                    
            }finally{
                    
            }
            context.write(chave, valorSaida);
        }

    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2021/SEM1/gabriela.gondo/Desktop/ImplementacaoLocalMR/OperacoesComerciais/Informacao7/TransacoesPorMercadoriaPeso";
        
        //Caso seja passado arquivos externamente, reescrevendo o valor das variáveis
        //Caso contrário, será feito teste local
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade7");
        
        job.setJarByClass(Informacao7a.class);
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
