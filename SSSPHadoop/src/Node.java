import java.util.ArrayList;

/**
 * @author Vishal Doshi
 *
 */

public class Node {
	public static enum Color{WHITE,GRAY,BLACK};
	private int id;
	private ArrayList<Integer> neighbors = new ArrayList<Integer>();
	private int distance;
//	private char color= 'W';
	private Color color = Color.WHITE;
	private int parent;
	
	//Constructor 1
	Node(int id){ //Called from Reducer
		this.id= id;
		distance=Integer.MAX_VALUE;
		parent = Integer.MAX_VALUE;
	}
	
	//Constructor 2 (Parameterized)
	Node(int id, int distance, int parent){ // IPD Child of currently explored node
		this.id=id;
		this.distance = distance;
		color= Color.GRAY;
		this.parent=parent;	
	}
	//Contructor 3 (Parameterized)
	Node(String nodeString){
		//Splitting the ID from the rest of information
		//id<tab>neighbors  distance     color   parent
		// 1<tab>  2,3      |    0    |   GRAY  | source
		// 2<tab>1,3,4,5 |Integer.MAX_VALUE|WHITE|null
		String[] idInfo = nodeString.split("\t");
		id = Integer.parseInt(idInfo[0]);
		
		String[] infoDetails = idInfo[1].split("\\|");
		
		//0 Putting the neighbors in the list
			for(String s: infoDetails[0].split(",")){
				if(s.length()>0){
					neighbors.add(Integer.parseInt(s));
				}
			}
		//1 Putting distance
		if(infoDetails[1].equals("Integer.MAX_VALUE")){
			distance = Integer.MAX_VALUE;
		}
		else{
			distance = Integer.parseInt(infoDetails[1]);
		}
		//2 Putting color
		color = Color.valueOf(infoDetails[2]);
		//3 Putting parent
		if(infoDetails[3].equals("null")){
			parent = Integer.MAX_VALUE;
		}
		else if(infoDetails[3].equals("source")){
			parent=0;
		}
		else{
			parent = Integer.parseInt(infoDetails[3]);
		}
	}
	
//Returns the object inform of String excluding the ID
	public String toString(){
		String s="";
		int n = neighbors.size();
		for(int i=0; i<n ; i++){
			s+=Integer.toString(neighbors.get(i));
			if(i!=(n-1)){
				s+=","; //Skip adding comma after last neighbor.
			}
		}
		if(distance<Integer.MAX_VALUE)
			s+="|"+distance+"|";
		else
			s+="|Integer.MAX_VALUE|";

		//Concatenate Color
		s+=color.toString();
		
		//Parent
		if(parent == 0)
			s+="|source";
		else if(parent<Integer.MAX_VALUE)
			s+="|"+parent;
		else
			s+="|null";
		return s;
	}
	
	//Getter and setters for Node fields.
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public ArrayList<Integer> getNeighbors() {
		return neighbors;
	}
	public void setNeighbors(ArrayList<Integer> neighbours) {
		this.neighbors = neighbours;
	}
	public int getDistance() {
		return distance;
	}
	public void setDistance(int distance) {
		this.distance = distance;
	}

	public Color getColor() {
		return color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public int getParent() {
		return parent;
	}

	public void setParent(int parent) {
		this.parent = parent;
	}
}
