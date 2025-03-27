import random
import copy

class Cell:
    def __init__(self, around_mines, mine):
        self.around_mines = around_mines
        self.mine = mine
        self.fl_open = False


class GamePole:
    def __init__(self, n, m):
        self.n = n
        self.m = m
        self._pole = self.init()
    
    def init(self):
        matrix = self._generate_cells()
        return matrix
    
    def show(self):
        for row in self._pole:
            display = []
            for cell in row:
                if cell.fl_open:
                    if cell.mine:
                        display.append('*')
                    else:
                        display.append(str(cell.around_mines))
                else:
                    display.append('#')
            print(' '.join(display))
    
    def _generate_mine_matrix(self):
        """Генерация матрицы с случайными бомбами"""
        matrix = [['#' for _ in range(self.n)] for _ in range(self.n)] 
        count_mine = 0
        while count_mine != self.m:
            i = random.randint(0, self.n-1)
            j = random.randint(0, self.n-1)
            if matrix[i][j] == '#':
                matrix[i][j] = '*'
                count_mine += 1
        return matrix
    
    def _generate_cells(self):
        """Генерация матрицы из обхектов Cell"""
        matrix = self._generate_mine_matrix()
        new_matrix = []

        for i in range(self.n):
            row = []
            for j in range(self.n):
                mine = matrix[i][j] == '*'
                count = 0
                for dx in [-1, 0, 1]:
                    for dy in [-1, 0, 1]:
                        if dx == 0 and dy == 0:
                            continue
                        x, y = i + dx, j + dy
                        if 0 <= x < self.n and 0 <= y < self.n:
                            if matrix[x][y] == '*':
                                count += 1
                row.append(Cell(around_mines=count, mine=mine))
            new_matrix.append(row)

        return new_matrix




