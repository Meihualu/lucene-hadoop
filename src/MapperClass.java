import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.*;
import java.util.HashMap;


public class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> {

    DocumentBuilder _docBuilder;
    private final static DocumentBuilderFactory _dbf = DocumentBuilderFactory.newInstance();
    private InputSource _xmlSource = null;
    private Document _doc = null;
    private NodeList _wayList =  null;
    private Node _node = null;
    private Element _nodeElement = null;
    private Node _way = null;
    private Element _wayElement = null;
    private String _id = null;
    private static final Logger LOG = LoggerFactory.getLogger(HadoopUtils.class);

    /**
     *
     * @param context
     * @throws IOException
     */
    public void setup(Context context) throws IOException {

        Configuration config = context.getConfiguration();
        try {
            _docBuilder = _dbf.newDocumentBuilder();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param key
     * @param xml
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(LongWritable key, Text xml, Context context)
            throws IOException, InterruptedException {

        try {
            _id = null;
            _xmlSource = new InputSource();
            _xmlSource.setCharacterStream(new StringReader(xml.toString()));
            _doc = _docBuilder.parse(_xmlSource);

            if(_doc.getElementsByTagName("node").getLength() > 0) {
                _node = _doc.getElementsByTagName("node").item(0);
                if (_node.getNodeType() == Node.ELEMENT_NODE) {
                    _nodeElement = (Element) _node;
                    _id = _nodeElement.getAttribute("id");
                    context.write(new LongWritable(Long.parseLong(_id)), xml);
                }
            } else {
                if (_doc.getElementsByTagName("way").getLength() > 0) {
                    _way = _doc.getElementsByTagName("way").item(0);
                    if (_way.getNodeType() == Node.ELEMENT_NODE) {
                        _wayElement = (Element) _way;
                        _id = _wayElement.getAttribute("id");
                        context.write(new LongWritable(Long.parseLong(_id)), xml);
                    }
                }
            }
        }catch (SAXException e) {
            LOG.error(e.getMessage());
        }
    }
}
